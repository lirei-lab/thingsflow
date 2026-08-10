package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	metricsPkg "flow-core/internal/metrics"

	"flow-core/internal/alarmcomment"
	"flow-core/internal/asset"
	"flow-core/internal/customer"
	"flow-core/internal/dashboard"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/device"
	"flow-core/internal/devicejwt"
	"flow-core/internal/entityquery"
	"flow-core/internal/entityview"
	"flow-core/internal/httputil"
	"flow-core/internal/oidc"
	"flow-core/internal/ota"
	"flow-core/internal/policy"
	"flow-core/internal/provisioning"
	"flow-core/internal/relations"
	"flow-core/internal/resource"
	"flow-core/internal/rpc"
	"flow-core/internal/system"
	telemetry "flow-core/internal/telemetry"
	"flow-core/internal/tenant"
	"flow-core/internal/tenantprofile"
	"flow-core/internal/topology"
	"flow-core/internal/transport"
	"flow-core/internal/twin"
	"flow-core/internal/twinmodel"
	"flow-core/internal/twinstore"
	"flow-core/internal/user"
	"flow-core/internal/widget"
	"flow-core/internal/ws"
)

// registerRoutes wires every route onto mux.
//
// Split out of startHTTPServer so the routing table can be built without
// binding a port. That matters more than tidiness: Go's ServeMux panics at
// *registration* when two patterns overlap without one being more specific, so
// a conflicting route is a startup crash and a total outage. A test that calls
// this function turns that into a failing build instead.
func registerRoutes(mux *http.ServeMux, allowedOrigin string) {

	mux.HandleFunc("/api/ws/plugins/telemetry", ws.HandleWebSocket)
	mux.HandleFunc("/api/ws", ws.HandleWebSocket) // TB UI v3.7+ uses this shorter path

	// Prometheus exposition format. Hand-rolled package — see
	// internal/metrics for the rationale (small surface, no deps).
	mux.HandleFunc("/metrics", metricsPkg.Handler)

	// Health endpoints for Kubernetes liveness + readiness probes.
	// /health: process is up — always 200 unless we panicked already.
	// /ready : dependencies are up — postgres reachable. Returns 503
	// during startup so k8s holds traffic off the pod until init finishes.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"UP"}`))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if dbpkg.Pool == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"DOWN","reason":"postgres pool not initialized"}`))
			return
		}
		stats := dbpkg.Pool.Stats()
		// Postgres pool: ping with a sub-probe timeout so the handler can
		// return diagnostics before Kubernetes' readiness client times out.
		pingCtx, pingCancel := context.WithTimeout(r.Context(), 750*time.Millisecond)
		defer pingCancel()
		if err := dbpkg.Pool.PingContext(pingCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "DOWN",
				"reason": "postgres unreachable",
				"postgresPool": map[string]interface{}{
					"open":      stats.OpenConnections,
					"inUse":     stats.InUse,
					"idle":      stats.Idle,
					"waitCount": stats.WaitCount,
				},
			})
			return
		}
		if strings.EqualFold(strings.TrimSpace(getEnv("TWIN_STATE_STORE", "")), "nats") && twinstore.Global() == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "DOWN",
				"reason": "nats twin state unavailable",
				"postgresPool": map[string]interface{}{
					"open":      stats.OpenConnections,
					"inUse":     stats.InUse,
					"idle":      stats.Idle,
					"waitCount": stats.WaitCount,
				},
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "UP",
			"postgresPool": map[string]interface{}{
				"open":      stats.OpenConnections,
				"inUse":     stats.InUse,
				"idle":      stats.Idle,
				"waitCount": stats.WaitCount,
			},
		})
	})

	// ─── Authentication ─────────────────────────────────────────────────
	mux.HandleFunc("/api/auth/login", cors(allowedOrigin, user.HandleLogin))

	// ─── HTTP device transport ───────────────────────────────────────
	// Public TB-style device provisioning. Register the exact path before
	// the tokenized /api/v1/{token}/... handler so "provision" is not
	// treated as a device access token.
	mux.HandleFunc("/api/v1/provision", cors(allowedOrigin, provisioning.HandleProvision))
	mux.HandleFunc("/api/v1/devices/me/jwt/refresh", cors(allowedOrigin, device.HandleDeviceJwtRefresh))

	// /api/v1/{token}/{telemetry|attributes|...} — TB classic compatible.
	// Authentication is per-request via the token segment, not JWT.
	mux.HandleFunc("/api/v1/", cors(allowedOrigin, transport.Handle))

	mux.HandleFunc("/api/auth/user", cors(allowedOrigin, user.HandleAuthUser))
	mux.HandleFunc("/api/auth/token", cors(allowedOrigin, user.HandleTokenRefresh))
	mux.HandleFunc("/api/auth/changePassword", cors(allowedOrigin, user.HandleChangePassword))
	mux.HandleFunc("/api/auth/logout", cors(allowedOrigin, user.HandleLogout))
	// ─── No-auth password reset / activation flow ───────────────────────
	mux.HandleFunc("/api/noauth/device-jwks", cors(allowedOrigin, devicejwt.HandleJWKS))
	mux.HandleFunc("/api/noauth/device-jwt-public.pem", cors(allowedOrigin, devicejwt.HandlePublicPEM))
	mux.HandleFunc("/.well-known/thingsflow-device-jwks.json", cors(allowedOrigin, devicejwt.HandleJWKS))
	mux.HandleFunc("/.well-known/thingsflow-device-public.pem", cors(allowedOrigin, devicejwt.HandlePublicPEM))
	mux.HandleFunc("/api/noauth/resetPasswordByEmail", cors(allowedOrigin, user.HandleResetPasswordByEmail))
	mux.HandleFunc("/api/noauth/resetPassword", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			user.HandleResetPasswordCheck(w, r)
			return
		}
		user.HandleResetPassword(w, r)
	})
	mux.HandleFunc("/api/noauth/activate", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			user.HandleActivateCheck(w, r)
			return
		}
		user.HandleActivate(w, r)
	})
	// ─── 2FA ────────────────────────────────────────────────────────────
	mux.HandleFunc("/api/auth/2fa/providers", cors(allowedOrigin, system.HandleTwoFaProviders))
	// Onboarding, mail presets and 2FA enrolment. Several answer 501 rather than
	// 200: this platform has no outbound mail transport, and a silent success
	// would leave an operator believing an invitation or code had been sent.
	mux.HandleFunc("/api/auth/2fa/verification/send", cors(allowedOrigin, system.HandleTwoFaVerificationSend))
	mux.HandleFunc("/api/auth/2fa/verification/check", cors(allowedOrigin, system.HandleTwoFaVerificationCheck))
	mux.HandleFunc("/api/2fa/account/config/generate", cors(allowedOrigin, system.HandleTwoFaAccountConfigGenerate))
	mux.HandleFunc("/api/2fa/account/config/submit", cors(allowedOrigin, system.HandleTwoFaAccountConfigSubmit))
	mux.HandleFunc("POST /api/user/sendActivationMail", cors(allowedOrigin, system.HandleSendActivationMail))
	mux.HandleFunc("/api/noauth/resendEmailActivation", cors(allowedOrigin, system.HandleResendEmailActivation))
	mux.HandleFunc("/api/noauth/activateByEmailCode", cors(allowedOrigin, system.HandleActivateByEmailCode))
	mux.HandleFunc("/api/mail/config/template", cors(allowedOrigin, system.HandleMailConfigTemplate))
	// Public dashboards are not enabled: there is no anonymous customer, so a
	// public login would mint a token bound to nothing. Refusing is safer than
	// issuing a credential whose scope is undefined.
	mux.HandleFunc("/api/auth/login/public", cors(allowedOrigin, system.HandlePublicLogin))
	// Asset CSV import reuses the device importer's row handling; the columns and
	// the per-row error accounting are the same shape.
	mux.HandleFunc("POST /api/asset/bulk_import", cors(allowedOrigin, system.HandleAssetBulkImport))
	mux.HandleFunc("/api/2fa/account/config", cors(allowedOrigin, system.HandleTwoFaAccountConfig))
	mux.HandleFunc("/api/2fa/account/config/providers", cors(allowedOrigin, system.HandleTwoFaAccountConfigProviders))
	mux.HandleFunc("/api/2fa/account/settings", cors(allowedOrigin, system.HandleTwoFaAccountSettings))
	mux.HandleFunc("/api/2fa/providers", cors(allowedOrigin, system.HandleTwoFaProvidersPage))
	mux.HandleFunc("/api/2fa/settings", cors(allowedOrigin, system.HandleTwoFaSettings))
	// ─── OAuth2 client CRUD ────────────────────────────────────────────
	mux.HandleFunc("/api/oauth2/client", cors(allowedOrigin, system.HandleOAuth2Client))
	// UI v3.7+ uses camelCase paths; older releases use slashed paths.
	// Register both shapes for the OAuth providers list and templates.
	mux.HandleFunc("/api/oauth2/clientInfos", cors(allowedOrigin, system.HandleOAuth2ClientInfos))
	mux.HandleFunc("/api/oauth2/configTemplates", cors(allowedOrigin, system.HandleOAuth2ConfigTemplate))
	// /api/notifications/stats — header bell badge counts. Empty inbox.
	mux.HandleFunc("/api/notifications/stats", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		if _, err := httputil.ExtractToken(r); err != nil {
			httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
			return
		}
		httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
			"totalUnreadCount":          0,
			"sequenceNumber":            0,
			"latestUnreadNotifications": []interface{}{},
		})
	}))
	// ─── Admin settings save + test ────────────────────────────────────
	// testMail/testSms are literals, so they win over {key} as the more
	// specific match. HandleAdminSettings reads the key from the path.
	mux.HandleFunc("GET /api/admin/settings", cors(allowedOrigin, system.HandleAdminSettings))
	mux.HandleFunc("POST /api/admin/settings", cors(allowedOrigin, system.HandleAdminSettingsSave))
	mux.HandleFunc("PUT /api/admin/settings", cors(allowedOrigin, system.HandleAdminSettingsSave))
	mux.HandleFunc("POST /api/admin/settings/testMail", cors(allowedOrigin, system.HandleAdminSettingsTestMail))
	mux.HandleFunc("POST /api/admin/settings/testSms", cors(allowedOrigin, system.HandleAdminSettingsTestSms))
	mux.HandleFunc("GET /api/admin/settings/{key}", cors(allowedOrigin, system.HandleAdminSettings))
	mux.HandleFunc("POST /api/admin/settings/{key}", cors(allowedOrigin, system.HandleAdminSettingsSave))
	mux.HandleFunc("PUT /api/admin/settings/{key}", cors(allowedOrigin, system.HandleAdminSettingsSave))
	// Legacy aliases the UI emits for the same admin_settings rows.
	// /api/admin/{jwtSettings,mailSettings,mailTemplate,securitySettings}
	// are equivalent to /api/admin/settings/{jwt,mail,mailTemplate,security}.
	for _, alias := range []struct{ uiPath, settingsKey string }{
		{"/api/admin/jwtSettings", "jwt"},
		{"/api/admin/mailSettings", "mail"},
		{"/api/admin/mailTemplate", "mailTemplate"},
		{"/api/admin/securitySettings", "securitySettings"},
		{"/api/admin/twoFaSettings", "twoFaSettings"},
		{"/api/admin/sms/config", "sms"},
	} {
		key := alias.settingsKey
		mux.HandleFunc(alias.uiPath, cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
			r.URL.Path = "/api/admin/settings/" + key
			if r.Method == "POST" || r.Method == "PUT" {
				system.HandleAdminSettingsSave(w, r)
				return
			}
			system.HandleAdminSettings(w, r)
		}))
	}
	mux.HandleFunc("/api/admin/repositorySettings", cors(allowedOrigin, system.HandleRepositorySettings))
	mux.HandleFunc("/api/admin/repositorySettings/checkAccess", cors(allowedOrigin, system.HandleRepositorySettingsCheckAccess))
	// /api/admin/updates — version banner check. UI emits this on every
	// page load; return "no update" so the banner stays hidden.
	mux.HandleFunc("/api/admin/updates", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
			"updateAvailable": false,
			"currentVersion":  "flow-core",
			"latestVersion":   "flow-core",
		})
	}))
	// ─── Notification CRUD ─────────────────────────────────────────────
	mux.HandleFunc("/api/notification/target", cors(allowedOrigin, system.HandleNotificationTarget))
	mux.HandleFunc("/api/notification/template", cors(allowedOrigin, system.HandleNotificationTemplate))
	mux.HandleFunc("/api/notification/request", cors(allowedOrigin, system.HandleNotificationRequestSave))
	mux.HandleFunc("/api/notification/request/preview", cors(allowedOrigin, system.HandleNotificationRequestPreview))
	mux.HandleFunc("/api/notifications/read", cors(allowedOrigin, system.HandleNotificationsRead))
	// ─── Calculated fields ─────────────────────────────────────────────
	mux.HandleFunc("/api/calculatedField", cors(allowedOrigin, system.HandleCalculatedField))
	// UI uses the plural form for the list page; map to the same handler.
	mux.HandleFunc("/api/calculatedFields", cors(allowedOrigin, system.HandleCalculatedField))
	mux.HandleFunc("POST /api/calculatedField/testScript", cors(allowedOrigin, system.HandleCalculatedFieldTestScript))
	mux.HandleFunc("/api/calculatedFields/names", cors(allowedOrigin, system.HandleCalculatedFieldNames))
	// Methodless: the handler answers 405 to anything but POST, which the
	// contract expects (a 404 would mean "endpoint missing").
	mux.HandleFunc("/api/calculatedField/{id}/debug", cors(allowedOrigin, system.HandleCalculatedFieldDebug))
	mux.HandleFunc("GET /api/calculatedField/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleCalculatedFieldByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/calculatedField/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleCalculatedFieldByID(w, r, r.PathValue("id"))
	}))
	// ─── Edge ──────────────────────────────────────────────────────────
	mux.HandleFunc("/api/edge", cors(allowedOrigin, system.HandleEdge))
	mux.HandleFunc("/api/edge/bulk_import", cors(allowedOrigin, system.HandleEdgeBulkImport))
	// ─── Mobile app + bundle ──────────────────────────────────────────
	mux.HandleFunc("/api/mobile/app", cors(allowedOrigin, system.HandleMobileApp))
	mux.HandleFunc("/api/mobile/bundle", cors(allowedOrigin, system.HandleMobileBundle))
	mux.HandleFunc("/api/mobile/qr/deepLink", cors(allowedOrigin, system.HandleMobileQrDeepLink))
	// ─── Tenant profile ───────────────────────────────────────────────
	mux.HandleFunc("/api/tenantProfiles", cors(allowedOrigin, tenantprofile.List))
	mux.HandleFunc("/api/tenantProfileInfos", cors(allowedOrigin, tenantprofile.List))
	mux.HandleFunc("POST /api/tenantProfile", cors(allowedOrigin, tenantprofile.Save))
	// Method+wildcard patterns: unmatched methods and malformed paths fall to
	// the /api/ catch-all (OPTIONS preflight included), which answers loudly
	// for anything not declared in the UI contract.
	mux.HandleFunc("POST /api/tenantProfile/{id}/default", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenantprofile.SetDefault(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/tenantProfile/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenantprofile.ByID(w, r, r.PathValue("id"), true)
	}))
	mux.HandleFunc("DELETE /api/tenantProfile/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenantprofile.Delete(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/tenantProfileInfo/default", cors(allowedOrigin, system.HandleTenantProfileInfoDefault))
	mux.HandleFunc("GET /api/tenantProfileInfo/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenantprofile.ByID(w, r, r.PathValue("id"), false)
	}))
	// ─── Audit logs by dimension ──────────────────────────────────────
	// The handler parses the dimension out of r.URL.Path. The UI calls both
	// the bare dimension (id in the query string) and the id-in-path form,
	// and entity takes either {id} or {type}/{id} — the contract gate
	// exercises all of these shapes.
	mux.HandleFunc("GET /api/audit/logs/customer", cors(allowedOrigin, system.HandleAuditLogsByDimension))
	mux.HandleFunc("GET /api/audit/logs/customer/{id}", cors(allowedOrigin, system.HandleAuditLogsByDimension))
	mux.HandleFunc("GET /api/audit/logs/user", cors(allowedOrigin, system.HandleAuditLogsByDimension))
	mux.HandleFunc("GET /api/audit/logs/user/{id}", cors(allowedOrigin, system.HandleAuditLogsByDimension))
	mux.HandleFunc("GET /api/audit/logs/entity/{id}", cors(allowedOrigin, system.HandleAuditLogsByDimension))
	mux.HandleFunc("GET /api/audit/logs/entity/{type}/{id}", cors(allowedOrigin, system.HandleAuditLogsByDimension))
	// ─── Image upload + import ────────────────────────────────────────
	mux.HandleFunc("/api/image", cors(allowedOrigin, resource.ImageUpload))
	mux.HandleFunc("/api/image/import", cors(allowedOrigin, resource.ImageImport))
	// Resource upload (POST) handled by /api/resource handler below; download is split
	mux.HandleFunc("/api/resource/upload", cors(allowedOrigin, resource.Upload))

	// ─── System bootstrap params ─────────────────────────────────────────
	mux.HandleFunc("/api/system/params", cors(allowedOrigin, system.HandleSystemParams))
	mux.HandleFunc("GET /api/dashboard/serverTime", cors(allowedOrigin, system.HandleServerTime))
	mux.HandleFunc("GET /api/dashboard/home", cors(allowedOrigin, dashboard.Home))
	mux.HandleFunc("/api/admin/featuresInfo", cors(allowedOrigin, system.HandleFeaturesInfo))
	mux.HandleFunc("/api/admin/topology/consistency", cors(allowedOrigin, topology.HandleConsistency))
	mux.HandleFunc("/api/admin/topology/backfill", cors(allowedOrigin, topology.HandleBackfill))
	// GET/PUT/POST — the methods HandleUserSettings itself dispatches. The
	// keyed form /api/user/settings/{KEY} lives in the /api/user/{a}/{b}
	// pattern further down.
	mux.HandleFunc("GET /api/user/settings", cors(allowedOrigin, system.HandleUserSettings))
	mux.HandleFunc("PUT /api/user/settings", cors(allowedOrigin, system.HandleUserSettings))
	mux.HandleFunc("POST /api/user/settings", cors(allowedOrigin, system.HandleUserSettings))
	mux.HandleFunc("GET /api/user/tokenAccessEnabled", cors(allowedOrigin, system.HandleUserTokenAccessEnabled))
	mux.HandleFunc("/api/usage", cors(allowedOrigin, system.HandleUsage))

	// ─── Dashboard API ───────────────────────────────────────────────────
	mux.HandleFunc("GET /api/tenant/dashboards", cors(allowedOrigin, dashboard.ListByTenant))
	// /api/dashboards (sysadmin global view) and /api/customer/dashboards
	// (customer-user view) both share the tenant-scoped lister; the JWT
	// scope (SYS_ADMIN / TENANT_ADMIN / CUSTOMER_USER) determines what
	// dashboard.ListByTenant filters internally.
	mux.HandleFunc("/api/dashboards", cors(allowedOrigin, dashboard.ListByTenant))
	mux.HandleFunc("GET /api/customer/dashboards", cors(allowedOrigin, dashboard.ListByTenant))
	mux.HandleFunc("/api/dashboard", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "POST" || r.Method == "PUT" {
			dashboard.Save(w, r)
			return
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	})
	// GET two-segment dashboard paths share one pattern because their pure
	// wildcard forms (info/{id} vs {id}/customers) overlap at
	// /api/dashboard/info/customers, which ServeMux rejects at registration.
	//   info/{id}      — same object; the UI uses the info variant when it
	//                    only needs the header, not the widget configuration.
	//   {id}/customers — the customers a dashboard is assigned to.
	mux.HandleFunc("GET /api/dashboard/{a}/{b}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		a, b := r.PathValue("a"), r.PathValue("b")
		switch {
		case a == "info":
			dashboard.ByID(w, r, b)
		case b == "customers":
			dashboard.ListCustomers(w, r, a)
		default:
			notImplemented(w, r)
		}
	}))
	// POST on /customers replaces the whole assigned set; add/remove adjust it.
	mux.HandleFunc("POST /api/dashboard/{id}/customers", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleDashboardCustomersBulk(w, r, r.PathValue("id"), "replace",
			customer.HandleAssignDashboardToCustomer)
	}))
	mux.HandleFunc("POST /api/dashboard/{id}/customers/add", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleDashboardCustomersBulk(w, r, r.PathValue("id"), "add",
			customer.HandleAssignDashboardToCustomer)
	}))
	mux.HandleFunc("POST /api/dashboard/{id}/customers/remove", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleDashboardCustomersBulk(w, r, r.PathValue("id"), "remove",
			customer.HandleAssignDashboardToCustomer)
	}))
	mux.HandleFunc("GET /api/dashboard/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		dashboard.ByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/dashboard/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		dashboard.Delete(w, r, r.PathValue("id"))
	}))

	// ─── Device API ──────────────────────────────────────────────────────
	mux.HandleFunc("GET /api/device/types", cors(allowedOrigin, device.HandleDeviceTypes))
	mux.HandleFunc("GET /api/tenant/deviceInfos", cors(allowedOrigin, device.HandleTenantDeviceInfos))
	// Two-segment device paths share one pattern: the wildcard forms
	// (info/{id} vs {id}/security etc.) overlap at e.g.
	// /api/device/info/security, which ServeMux rejects at registration.
	// Each target handler dispatches (and 405s) its own methods.
	mux.HandleFunc("/api/device/{a}/{b}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		a, b := r.PathValue("a"), r.PathValue("b")
		switch {
		case a == "info":
			device.HandleDeviceInfoById(w, r, b)
		case b == "security":
			device.HandleDeviceSecurity(w, r, a)
		case b == "credentials":
			device.HandleDeviceCredentials(w, r, a)
		case b == "jwt":
			device.HandleDeviceJwt(w, r, a)
		default:
			notImplemented(w, r)
		}
	}))
	mux.HandleFunc("POST /api/device", cors(allowedOrigin, device.HandleDeviceCreateOrUpdate))
	mux.HandleFunc("PUT /api/device", cors(allowedOrigin, device.HandleDeviceCreateOrUpdate))
	mux.HandleFunc("POST /api/device/credentials", cors(allowedOrigin, device.HandleDeviceCredentialsUpdate))
	mux.HandleFunc("PUT /api/device/credentials", cors(allowedOrigin, device.HandleDeviceCredentialsUpdate))
	mux.HandleFunc("POST /api/device/bulk_import", cors(allowedOrigin, device.HandleDeviceBulkImport))
	mux.HandleFunc("GET /api/device/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		device.HandleDeviceById(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/device/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		device.HandleDeviceDelete(w, r, r.PathValue("id"))
	}))
	// /api/device-with-credentials — the UI "Add device" wizard's one-shot create with a
	// caller-supplied credential ({device, credentials}).
	mux.HandleFunc("/api/device-with-credentials", cors(allowedOrigin, device.HandleDeviceWithCredentials))
	// /api/device-connectivity/{id} — backs the UI "Check connectivity" dialog with real
	// snippets (device JWT as the credential, pinned MQTT client id, ts-bearing payload).
	mux.HandleFunc("GET /api/device-connectivity/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		device.HandleDeviceConnectivity(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("/api/devices", cors(allowedOrigin, device.HandleDevicesByIds))
	mux.HandleFunc("GET /api/devices/count/{entityType}/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		if _, err := httputil.ExtractToken(r); err != nil {
			httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
			return
		}
		if r.PathValue("entityType") != "DEVICE_PROFILE" {
			httputil.WriteJSON(w, http.StatusOK, 0)
			return
		}
		var count int
		if err := dbpkg.Pool.QueryRow("SELECT count(*) FROM device WHERE device_profile_id = $1", r.PathValue("id")).Scan(&count); err != nil {
			httputil.WriteJSON(w, http.StatusOK, 0)
			return
		}
		httputil.WriteJSON(w, http.StatusOK, count)
	}))

	// ─── Widget API ──────────────────────────────────────────────────────
	mux.HandleFunc("/api/widgetType", cors(allowedOrigin, widget.Type))
	// /api/widgetType/{id} — open widget by id (UI hits this on click).
	// Methodless: the widget handlers are read-only and method-agnostic.
	mux.HandleFunc("/api/widgetType/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		widget.TypeByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("/api/widgetTypes", cors(allowedOrigin, widget.Types))
	// /api/widgetTypesInfos — paginated WidgetTypeInfo list. Used by the
	// dashboard widget picker.
	mux.HandleFunc("/api/widgetTypesInfos", cors(allowedOrigin, widget.TypesInfos))
	mux.HandleFunc("/api/widgetTypeInfo/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		widget.TypeInfoByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("/api/widgetTypeFqns", cors(allowedOrigin, widget.TypeFqns))
	mux.HandleFunc("/api/widgetsBundles", cors(allowedOrigin, widget.Bundles))
	// TB UI v4.x calls /api/widgetsBundles/all expecting a flat JSON
	// array (not the paginated wrapper). The widget gallery's .sort()
	// crashes if it gets the 404 error JSON instead.
	mux.HandleFunc("/api/widgetsBundles/all", cors(allowedOrigin, widget.BundlesFlat))
	mux.HandleFunc("/api/widgetsBundleByAlias/{alias}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		widget.BundleByAlias(w, r, r.PathValue("alias"))
	}))
	// POST/PUT save not supported yet. The methodless registration answers
	// 405 for them — the contract expects 405 ("not supported"), not 404
	// ("endpoint missing"); GET routes to the more specific pattern.
	mux.HandleFunc("GET /api/widgetsBundle", cors(allowedOrigin, widget.Bundles))
	mux.HandleFunc("/api/widgetsBundle", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	mux.HandleFunc("/api/widgetsBundle/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		widget.BundleByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("/api/widgetsBundle/{id}/widgetTypes", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		widget.BundleWidgetTypes(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("/api/widgetsBundle/{id}/widgetTypeFqns", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		widget.BundleWidgetTypeFqns(w, r, r.PathValue("id"))
	}))

	// ─── Entity Query API ────────────────────────────────────────────────
	mux.HandleFunc("/api/entitiesQuery/find", cors(allowedOrigin, entityquery.Find))
	mux.HandleFunc("/api/entitiesQuery/find/keys", cors(allowedOrigin, entityquery.FindKeys))
	mux.HandleFunc("/api/v2/entitiesQuery/find/keys", cors(allowedOrigin, entityquery.FindKeysV2))
	mux.HandleFunc("/api/entitiesQuery/count", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		system.HandleEntitiesQueryCount(w, r)
	})

	// ─── Per-entity calculated fields, events, v2 alarms ──────────────
	// /api/{entityType}/{id}/calculatedFields, /api/events/{entityType}/{id},
	// /api/v2/alarm/{entityType}/{id}. Stub: empty page for the first two
	// (we don't compute or store these yet); v2 alarm aliases the existing
	// per-device alarm handler.
	// Events are not stored (no rule engine in flow-core); every shape the
	// UI posts to — {id}, {entityType}/{id}, {entityType}/{id}/clear —
	// gets the empty page so the panels render.
	eventsStub := cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.EmptyPageData(w)
	})
	mux.HandleFunc("/api/events/{a}", eventsStub)
	mux.HandleFunc("/api/events/{a}/{b}", eventsStub)
	mux.HandleFunc("/api/events/{a}/{b}/{c}", eventsStub)
	// The v2 alarm handler parses its own path; the bare form is registered
	// explicitly because without a subtree pattern there is no automatic
	// /api/v2/alarm → /api/v2/alarm/ redirect any more.
	mux.HandleFunc("/api/v2/alarm", cors(allowedOrigin, tenant.HandleAlarmRest))
	mux.HandleFunc("/api/v2/alarm/{a}", cors(allowedOrigin, tenant.HandleAlarmRest))
	mux.HandleFunc("/api/v2/alarm/{a}/{b}", cors(allowedOrigin, tenant.HandleAlarmRest))

	// ─── Tenant/Customer/User API ────────────────────────────────────────
	// One-segment literals under /api/tenant/ must carry a method: a
	// methodless literal alongside GET /api/tenant/{id} is a registration
	// conflict (startup panic).
	mux.HandleFunc("GET /api/tenant/profile", cors(allowedOrigin, system.HandleTenantProfile))
	mux.HandleFunc("GET /api/tenant/deviceProfiles", cors(allowedOrigin, system.HandleTenantDeviceProfiles))
	mux.HandleFunc("GET /api/tenant/assetProfiles", cors(allowedOrigin, system.HandleTenantAssetProfiles))
	// UI also calls these without the /tenant/ prefix. Same handler.
	mux.HandleFunc("/api/deviceProfiles", cors(allowedOrigin, system.HandleTenantDeviceProfiles))
	mux.HandleFunc("/api/assetProfiles", cors(allowedOrigin, system.HandleTenantAssetProfiles))
	mux.HandleFunc("GET /api/tenant/assets", cors(allowedOrigin, system.HandleTenantAssets))
	mux.HandleFunc("/api/assets", cors(allowedOrigin, system.HandleTenantAssets))
	mux.HandleFunc("GET /api/tenant/assetInfos", cors(allowedOrigin, system.HandleTenantAssetInfos))
	mux.HandleFunc("GET /api/tenant/entityViews", cors(allowedOrigin, system.HandleTenantEntityViews))
	mux.HandleFunc("/api/entityViews", cors(allowedOrigin, system.HandleTenantEntityViews))
	mux.HandleFunc("GET /api/tenant/entityViewInfos", cors(allowedOrigin, system.HandleTenantEntityViewInfos))
	mux.HandleFunc("/api/deviceProfileInfos", cors(allowedOrigin, system.HandleDeviceProfileInfos))
	mux.HandleFunc("/api/assetProfileInfos", cors(allowedOrigin, system.HandleAssetProfileInfos))
	// /api/customers — TB-Java path for the customers list (UI v3.7+ uses this).
	mux.HandleFunc("/api/customers", cors(allowedOrigin, system.HandleTenantCustomers))
	mux.HandleFunc("GET /api/tenant/customers", cors(allowedOrigin, system.HandleTenantCustomers))
	// /api/tenant/devices serves two contracts:
	//   ?deviceName=X       → SDK lookup-by-name (single device or 404)
	//   ?pageSize=&page=    → UI device list (paginated, same as deviceInfos)
	// The UI calls the list form on the Devices screen; SDKs call the
	// by-name form. Branch on which query param is present.
	mux.HandleFunc("GET /api/tenant/devices", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("deviceName") != "" {
			device.HandleTenantDeviceByName(w, r)
			return
		}
		device.HandleTenantDeviceInfos(w, r)
	}))
	mux.HandleFunc("GET /api/tenant/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenant.HandleTenantById(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/tenant/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenant.HandleTenantDelete(w, r, r.PathValue("id"))
	}))
	// Two-segment tenant paths share one pattern (info/{id} vs {id}/users
	// overlap as pure wildcards, which ServeMux rejects at registration).
	mux.HandleFunc("GET /api/tenant/{a}/{b}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		a, b := r.PathValue("a"), r.PathValue("b")
		switch {
		case a == "info":
			// /api/tenant/info/{id} variant (controller declares both shapes)
			tenant.HandleTenantInfoById(w, r, b)
		case b == "dashboards":
			system.HandleTenantDashboardsByID(w, r, a)
		case b == "users":
			system.HandleTenantUsersByID(w, r, a)
		default:
			notImplemented(w, r)
		}
	}))
	// SYS_ADMIN-scoped tenant CRUD.
	mux.HandleFunc("POST /api/tenant", cors(allowedOrigin, tenant.HandleTenantSave))
	mux.HandleFunc("PUT /api/tenant", cors(allowedOrigin, tenant.HandleTenantSave))
	mux.HandleFunc("/api/tenants", cors(allowedOrigin, tenant.HandleTenantsList))
	mux.HandleFunc("/api/tenantInfos", cors(allowedOrigin, tenant.HandleTenantsList))
	mux.HandleFunc("GET /api/tenantInfo/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenant.HandleTenantInfoById(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("POST /api/customer", cors(allowedOrigin, customer.Save))
	mux.HandleFunc("PUT /api/customer", cors(allowedOrigin, customer.Save))
	mux.HandleFunc("GET /api/customer/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenant.HandleCustomerById(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/customer/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		customer.Delete(w, r, r.PathValue("id"))
	}))
	// Customer sub-resources. Two TB shapes share the two-segment form —
	//   GET    {customerId}/{kind}   — list entities assigned to the customer
	//   DELETE {kind}/{entityId}     — unassign (customer implied by the row)
	// — so each method gets its own wildcard pattern; the shapes only
	// coexist because their methods are disjoint.
	mux.HandleFunc("GET /api/customer/{id}/{kind}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		switch r.PathValue("kind") {
		case "devices", "deviceInfos":
			device.HandleCustomerDeviceInfos(w, r, id)
		case "dashboards":
			dashboard.ListByCustomer(w, r, id)
		case "users":
			system.HandleCustomerUsers(w, r, id)
		case "assetInfos", "assets":
			system.HandleCustomerAssetInfos(w, r, id)
		case "entityViewInfos", "entityViews":
			system.HandleCustomerEntityViewInfos(w, r, id)
		case "edgeInfos", "edges":
			system.HandleCustomerEdgeInfos(w, r, id)
		default:
			notImplemented(w, r)
		}
	}))
	mux.HandleFunc("DELETE /api/customer/{kind}/{entityId}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		customer.HandleAssignToCustomer(w, r, "", r.PathValue("kind"), r.PathValue("entityId"))
	}))
	// POST|DELETE /api/customer/{customerId}/{kind}/{entityId} — (un)assign.
	mux.HandleFunc("POST /api/customer/{customerId}/{kind}/{entityId}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		customerID, kind, entityID := r.PathValue("customerId"), r.PathValue("kind"), r.PathValue("entityId")
		if kind == "dashboard" {
			customer.HandleAssignDashboardToCustomer(w, r, customerID, entityID, true)
			return
		}
		customer.HandleAssignToCustomer(w, r, customerID, kind, entityID)
	}))
	mux.HandleFunc("DELETE /api/customer/{customerId}/{kind}/{entityId}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		customerID, kind, entityID := r.PathValue("customerId"), r.PathValue("kind"), r.PathValue("entityId")
		if kind == "dashboard" {
			customer.HandleAssignDashboardToCustomer(w, r, customerID, entityID, false)
			return
		}
		// unassign clears the column, so the customer id is dropped
		customer.HandleAssignToCustomer(w, r, "", kind, entityID)
	}))
	mux.HandleFunc("POST /api/user", cors(allowedOrigin, user.HandleUserCreateOrUpdate))
	mux.HandleFunc("PUT /api/user", cors(allowedOrigin, user.HandleUserCreateOrUpdate))
	mux.HandleFunc("GET /api/user/dashboards", cors(allowedOrigin, dashboard.UserList))
	// /api/user/dashboards/{id}/{action} — VISIT, etc. UI fires
	// these as analytics pings; we acknowledge with 200.
	mux.HandleFunc("/api/user/dashboards/{id}/{action}", cors(allowedOrigin, dashboard.Visit))
	mux.HandleFunc("GET /api/user/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		tenant.HandleUserById(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/user/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		user.HandleUserDelete(w, r, r.PathValue("id"))
	}))
	// Two-segment user paths share one pattern: settings/{key} vs
	// {id}/token overlap as pure wildcards (e.g. /api/user/settings/token),
	// which ServeMux rejects at registration.
	mux.HandleFunc("/api/user/{a}/{b}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		a, b := r.PathValue("a"), r.PathValue("b")
		switch {
		case a == "settings":
			// keyed settings (DOC_LINKS, QUICK_LINKS, GETTING_STARTED);
			// the handler dispatches GET/PUT/DELETE itself.
			system.HandleUserSettingsByKey(w, r, b)
		case b == "activationLink" && r.Method == http.MethodGet:
			user.HandleUserActivationLink(w, r, a)
		case b == "activationLinkInfo" && r.Method == http.MethodGet:
			system.HandleUserActivationLinkInfo(w, r, a)
		case b == "token" && r.Method == http.MethodGet:
			system.HandleUserToken(w, r, a)
		default:
			notImplemented(w, r)
		}
	}))

	// ─── Alarm API ───────────────────────────────────────────────────────
	mux.HandleFunc("/api/alarm/types", cors(allowedOrigin, system.HandleAlarmTypes))
	mux.HandleFunc("/api/alarms", cors(allowedOrigin, tenant.HandleAlarmRest))
	// /api/v2/alarms is the same paginated list endpoint with extra fields
	// (assignee, originatorLabel) that the modern UI prefers. We serve the
	// same handler — the response shape is a superset-compatible page.
	mux.HandleFunc("/api/v2/alarms", cors(allowedOrigin, tenant.HandleAlarmRest))
	// Alarm paths by shape. HandleAlarmRest parses its own path (id, info/,
	// ack, clear, assign, highestSeverity…), so the wildcards only pin the
	// shapes the old subtree accepted; comments get their own dispatch.
	mux.HandleFunc("/api/alarm/{id}", cors(allowedOrigin, tenant.HandleAlarmRest))
	mux.HandleFunc("/api/alarm/{a}/{b}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("b") == "comment" {
			alarmID := r.PathValue("a")
			switch r.Method {
			case "GET":
				alarmcomment.List(w, r, alarmID)
				return
			case "POST", "PUT":
				alarmcomment.Save(w, r, alarmID)
				return
			}
		}
		tenant.HandleAlarmRest(w, r)
	}))
	mux.HandleFunc("/api/alarm/{a}/{b}/{c}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("b") == "comment" && r.Method == "DELETE" {
			alarmcomment.Delete(w, r, r.PathValue("a"), r.PathValue("c"))
			return
		}
		tenant.HandleAlarmRest(w, r)
	}))

	// ─── Rule Chains ─────────────────────────────────────────────────────
	mux.HandleFunc("/api/ruleChains", cors(allowedOrigin, system.HandleRuleChains))
	mux.HandleFunc("/api/ruleChain", cors(allowedOrigin, system.HandleRuleChains))
	// (/api/ruleChain/autoAssignToEdgeRuleChains is registered with the edge
	// block further down.)
	// /api/ruleChain/{id} and /api/ruleChain/{id}/metaData — UI hits the
	// detail endpoint when the user clicks a chain in the list, then the
	// metaData endpoint to render the editor canvas. Without these the
	// chain shows in the list but errors when opened. UI versions disagree
	// on the casing — older builds emit `metaData` (camelCase as in the
	// Java DTO field), newer ones emit `metadata`. Both reach the same
	// handler; the UUID guard preserves the old router's 404 for junk ids.
	ruleChainMetaData := cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !httputil.LooksLikeUUID(id) {
			notImplemented(w, r)
			return
		}
		system.HandleRuleChainMetaData(w, r, id)
	})
	mux.HandleFunc("/api/ruleChain/{id}/metaData", ruleChainMetaData)
	mux.HandleFunc("/api/ruleChain/{id}/metadata", ruleChainMetaData)
	mux.HandleFunc("/api/ruleChain/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !httputil.LooksLikeUUID(id) {
			notImplemented(w, r)
			return
		}
		system.HandleRuleChainByID(w, r, id)
	}))

	// /api/components is hit by the UI when opening legacy rule-chain screens.
	// ThingsFlow does not ship a rule engine in flow-core; alarm detection runs in
	// Bento/NATS. Returning a small catalogue keeps TB UI compatibility without
	// implying rule-chain execution support.
	mux.HandleFunc("/api/components", cors(allowedOrigin, system.HandleRuleNodeComponents))

	// ─── Notification Rules ───────────────────────────────────────────────
	mux.HandleFunc("/api/notification/rule", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "POST" || r.Method == "PUT" {
			system.HandleNotificationRule(w, r)
			return
		}
		system.HandleNotificationRules(w, r)
	})
	// Alias for the plural variant the UI uses in some screens.
	mux.HandleFunc("/api/notification/rules", cors(allowedOrigin, system.HandleNotificationRules))
	mux.HandleFunc("/api/notification/requests", cors(allowedOrigin, system.HandleNotificationRequests))

	// ─── Asset/Device Profile by ID ───────────────────────────────────────
	mux.HandleFunc("POST /api/deviceProfile", cors(allowedOrigin, device.HandleDeviceProfileCreateOrUpdate))
	mux.HandleFunc("PUT /api/deviceProfile", cors(allowedOrigin, device.HandleDeviceProfileCreateOrUpdate))
	mux.HandleFunc("GET /api/deviceProfile/names", cors(allowedOrigin, system.HandleDeviceProfileNames))
	mux.HandleFunc("GET /api/assetProfile/names", cors(allowedOrigin, system.HandleAssetProfileNames))
	mux.HandleFunc("GET /api/deviceProfile/default/info", cors(allowedOrigin, system.HandleDefaultDeviceProfileInfo))
	// POST /api/deviceProfile/{id}/default — promote a profile. Done in one
	// transaction so the tenant is never momentarily without a default.
	mux.HandleFunc("POST /api/deviceProfile/{id}/default", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleSetDefaultProfile(w, r, "device_profile", "DEVICE_PROFILE", r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/deviceProfile/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleDeviceProfileById(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/deviceProfile/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		device.HandleDeviceProfileDelete(w, r, r.PathValue("id"))
	}))

	// ─── Asset CRUD ───────────────────────────────────────────────────────
	mux.HandleFunc("POST /api/asset", cors(allowedOrigin, asset.Save))
	mux.HandleFunc("PUT /api/asset", cors(allowedOrigin, asset.Save))
	// /api/asset/info/{id} — slim projection. UI asset list opens the
	// detail pane via this endpoint; same shape as GET /api/asset/{id}.
	mux.HandleFunc("GET /api/asset/info/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		asset.GetByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/asset/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		asset.GetByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/asset/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		asset.Delete(w, r, r.PathValue("id"))
	}))

	// ─── AssetProfile CRUD ────────────────────────────────────────────────
	mux.HandleFunc("POST /api/assetProfile", cors(allowedOrigin, asset.SaveProfile))
	mux.HandleFunc("PUT /api/assetProfile", cors(allowedOrigin, asset.SaveProfile))
	mux.HandleFunc("GET /api/assetProfile/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleAssetProfileByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/assetProfile/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		asset.DeleteProfile(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("POST /api/assetProfile/{id}/default", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleSetDefaultProfile(w, r, "asset_profile", "ASSET_PROFILE", r.PathValue("id"))
	}))

	// ─── EntityView CRUD ──────────────────────────────────────────────────
	mux.HandleFunc("/api/entityView", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "POST" || r.Method == "PUT" {
			entityview.Save(w, r)
			return
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	})
	// entityView is routed by method rather than by a prefix catch-all.
	//
	// The catch-all shape it replaces is what produced this subtree's bugs: it
	// handled DELETE and let every other method fall through to notImplemented,
	// which answers a GET with an empty page and HTTP 200 — so the detail screen
	// rendered blank and reported nothing. Declaring the methods makes an
	// undeclared one a 405 from the mux itself, which is visible.
	//
	// The literal /types route must also carry its method: a methodless literal
	// alongside a method+wildcard pattern is a registration conflict, and Go
	// panics on it at startup. routes_test.go builds the mux to catch that.
	mux.HandleFunc("GET /api/entityView/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		entityview.GetByID(w, r, r.PathValue("id"), false)
	}))
	mux.HandleFunc("DELETE /api/entityView/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		entityview.Delete(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("GET /api/entityView/info/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		entityview.GetByID(w, r, r.PathValue("id"), true)
	}))

	// ─── Resources / Images / SCADA Symbols ───────────────────────────────
	mux.HandleFunc("/api/images", cors(allowedOrigin, resource.Images))
	// PUT /api/images/{scope}/{key} updates bytes; any other method streams
	// them back (ImageData parses scope/key/preview from the path itself).
	imagesHandler := cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			resource.ImageUpdate(w, r)
			return
		}
		resource.ImageData(w, r)
	})
	mux.HandleFunc("/api/images/{scope}/{key}", imagesHandler)
	mux.HandleFunc("/api/images/{scope}/{key}/{extra}", imagesHandler)
	mux.HandleFunc("GET /api/resource", cors(allowedOrigin, resource.Resources))
	mux.HandleFunc("POST /api/resource", cors(allowedOrigin, resource.Upload))
	mux.HandleFunc("PUT /api/resource", cors(allowedOrigin, resource.Upload))
	mux.HandleFunc("/api/resource/js/{id}/download", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		resource.JsDownload(w, r, r.PathValue("id"))
	}))
	// /api/resource/tenant — the tenant's own resources, without the
	// system-seeded ones the other listings mix in.
	mux.HandleFunc("GET /api/resource/tenant", cors(allowedOrigin, resource.ListByTenant))
	mux.HandleFunc("/api/resource/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		switch r.Method {
		case "DELETE":
			resource.Delete(w, r, id)
		case "GET":
			resource.GetByID(w, r, id)
		default:
			notImplemented(w, r)
		}
	}))
	// Two-segment resource paths share one pattern: info/{id} vs {id}/info
	// overlap as pure wildcards (/api/resource/info/info), which ServeMux
	// rejects at registration.
	mux.HandleFunc("/api/resource/{a}/{b}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		a, b := r.PathValue("a"), r.PathValue("b")
		switch {
		case b == "download":
			resource.Download(w, r, a)
		case b == "info":
			resource.Info(w, r, a)
		case a == "info" && r.Method == http.MethodGet:
			// metadata only (the row holds the whole file)
			resource.GetByID(w, r, b)
		case b == "data" && r.Method == http.MethodPut:
			// replaces the stored bytes; the upload handler knows how
			resource.ImageUpdate(w, r)
		default:
			notImplemented(w, r)
		}
	}))

	// ─── Feature stubs and small UI bootstrap endpoints ──────────────────
	mux.HandleFunc("/api/admin/repositorySettings/info", cors(allowedOrigin, system.HandleRepositorySettingsInfo))
	mux.HandleFunc("/api/admin/repositorySettings/exists", cors(allowedOrigin, system.HandleRepositorySettingsExists))
	mux.HandleFunc("/api/admin/autoCommitSettings/exists", cors(allowedOrigin, system.HandleAutoCommitSettingsExists))
	// /api/noauth/oauth2Clients?platform=WEB — TB UI hits this on the login
	// page to render external auth buttons. With no OIDC env configured it
	// still returns [] so the existing password login screen stays clean.
	mux.HandleFunc("/api/noauth/oauth2Clients", cors(allowedOrigin, oidc.HandleOAuth2Clients))
	mux.HandleFunc("/api/noauth/oidc/authorize/{provider}", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		oidc.HandleAuthorize(w, r, r.PathValue("provider"))
	})
	mux.HandleFunc("/login/oauth2/code/{provider}", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		oidc.HandleCallback(w, r, r.PathValue("provider"))
	})
	mux.HandleFunc("/api/noauth/userPasswordPolicy", cors(allowedOrigin, system.HandleUserPasswordPolicy))
	mux.HandleFunc("/api/uiSettings/helpBaseUrl", cors(allowedOrigin, system.HandleUiHelpBaseUrl))
	mux.HandleFunc("/api/oauth2/loginProcessingUrl", cors(allowedOrigin, system.HandleOAuth2LoginProcessingUrl))
	mux.HandleFunc("/api/trendz/settings", cors(allowedOrigin, system.HandleTrendzSettings))
	mux.HandleFunc("/api/notification/settings", cors(allowedOrigin, system.HandleNotificationSettings))
	mux.HandleFunc("/api/notification/settings/user", cors(allowedOrigin, system.HandleNotificationSettingsUser))
	mux.HandleFunc("/api/notification/deliveryMethods", cors(allowedOrigin, system.HandleNotificationDeliveryMethods))
	mux.HandleFunc("/api/notification/targets", cors(allowedOrigin, system.HandleNotificationTargets))
	mux.HandleFunc("/api/notification/templates", cors(allowedOrigin, system.HandleNotificationTemplates))
	mux.HandleFunc("/api/deviceProfileInfo/default", cors(allowedOrigin, system.HandleDeviceProfileInfoDefault))
	mux.HandleFunc("/api/assetProfileInfo/default", cors(allowedOrigin, system.HandleAssetProfileInfoDefault))
	// /api/{device,asset}ProfileInfo/{id} — slim by-id projection used
	// by the UI when picking a profile in entity edit dialogs.
	mux.HandleFunc("/api/deviceProfileInfo/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleProfileInfoById(w, r, "device_profile", "DEVICE_PROFILE", r.PathValue("id"))
	}))
	mux.HandleFunc("/api/assetProfileInfo/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		system.HandleProfileInfoById(w, r, "asset_profile", "ASSET_PROFILE", r.PathValue("id"))
	}))
	// /api/alarmsQuery/find — POST body with EntityId/keyFilters; returns
	// the same alarm list shape /api/alarms produces.
	mux.HandleFunc("/api/alarmsQuery/find", cors(allowedOrigin, tenant.HandleAlarmsQueryFind))
	mux.HandleFunc("GET /api/asset/types", cors(allowedOrigin, system.HandleAssetTypes))
	mux.HandleFunc("GET /api/entityView/types", cors(allowedOrigin, system.HandleEntityViewTypes))
	mux.HandleFunc("/api/edge/types", cors(allowedOrigin, system.HandleEdgeTypes))
	// Rule node palette. TB classic returns available rule-engine component
	// classes. ThingsFlow keeps this empty because rules are not part of flow-core.
	mux.HandleFunc("/api/component/descriptors", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	})
	mux.HandleFunc("/api/component/descriptor", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("null"))
	})
	mux.HandleFunc("/api/edges", cors(allowedOrigin, system.HandleEdges))
	mux.HandleFunc("/api/otaPackages", cors(allowedOrigin, ota.List))
	// /api/otaPackage (POST/PUT) — create-or-update info (no binary).
	mux.HandleFunc("POST /api/otaPackage", cors(allowedOrigin, ota.SaveInfo))
	mux.HandleFunc("PUT /api/otaPackage", cors(allowedOrigin, ota.SaveInfo))
	// /api/otaPackage/{id}                — GET (info), DELETE (remove)
	// /api/otaPackage/{id} (POST multipart) — upload binary
	mux.HandleFunc("GET /api/otaPackage/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		ota.InfoByID(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("POST /api/otaPackage/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		ota.Upload(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("PUT /api/otaPackage/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		ota.Upload(w, r, r.PathValue("id"))
	}))
	mux.HandleFunc("DELETE /api/otaPackage/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		ota.Delete(w, r, r.PathValue("id"))
	}))
	// Two-segment ota paths share one pattern: info/{id} vs {id}/download
	// overlap as pure wildcards, which ServeMux rejects at registration.
	mux.HandleFunc("GET /api/otaPackage/{a}/{b}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		a, b := r.PathValue("a"), r.PathValue("b")
		switch {
		case a == "info":
			ota.InfoByID(w, r, b)
		case b == "download":
			ota.Download(w, r, a)
		default:
			notImplemented(w, r)
		}
	}))
	mux.HandleFunc("/api/queues", cors(allowedOrigin, system.HandleQueues))
	mux.HandleFunc("/api/ruleChain/autoAssignToEdgeRuleChains", cors(allowedOrigin, system.HandleRuleChainAutoAssign))
	mux.HandleFunc("/api/mobile/qr/settings", cors(allowedOrigin, system.HandleMobileQrSettings))
	mux.HandleFunc("/api/relations/info", cors(allowedOrigin, system.HandleRelationsInfo))

	// ─── Audit logs / OAuth2 / Mobile / Notifications ────────────────────
	mux.HandleFunc("/api/audit/logs", cors(allowedOrigin, system.HandleAuditLogs))
	// Alias for the camelCase variant the UI uses in some screens.
	mux.HandleFunc("/api/auditLogs", cors(allowedOrigin, system.HandleAuditLogs))
	// /api/audit/logs/{customer,user,entity}/{id} are registered higher up via
	// system.HandleAuditLogsByDimension.
	mux.HandleFunc("/api/oauth2/client/infos", cors(allowedOrigin, system.HandleOAuth2ClientInfos))
	mux.HandleFunc("/api/oauth2/config/template", cors(allowedOrigin, system.HandleOAuth2ConfigTemplate))
	mux.HandleFunc("/api/tenant/dashboard/home/info", cors(allowedOrigin, system.HandleTenantDashboardHomeInfo))
	mux.HandleFunc("/api/mobile/bundle/infos", cors(allowedOrigin, system.HandleMobileBundleInfos))
	mux.HandleFunc("/api/notifications", cors(allowedOrigin, system.HandleNotificationsCenter))

	// ─── Relations API ────────────────────────────────────────────────────
	mux.HandleFunc("/api/relation", cors(allowedOrigin, relations.Handle))
	mux.HandleFunc("/api/relations", cors(allowedOrigin, relations.Handle))

	// ─── Flow Twin API ─────────────────────────────────────────────────────
	// Method patterns are the executable OpenAPI contract. Methodless
	// fallbacks preserve the canonical JSON error envelope on wrong methods.
	mux.HandleFunc("GET /api/twin-models", cors(allowedOrigin, twinmodel.HandleCollection))
	mux.HandleFunc("POST /api/twin-models", cors(allowedOrigin, twinmodel.HandleCollection))
	mux.HandleFunc("/api/twin-models", cors(allowedOrigin, twinmodel.HandleCollection))
	mux.HandleFunc("GET /api/twin-models/{modelId}/{version}", cors(allowedOrigin, twinmodel.HandleVersion))
	mux.HandleFunc("DELETE /api/twin-models/{modelId}/{version}", cors(allowedOrigin, twinmodel.HandleVersion))
	mux.HandleFunc("/api/twin-models/{modelId}/{version}", cors(allowedOrigin, twinmodel.HandleVersion))
	mux.HandleFunc("PUT /api/twins/{entityType}/{entityId}/model", cors(allowedOrigin, policy.EnforceModelWrite(twin.PolicyContext, twinmodel.HandleRepoint)))
	mux.HandleFunc("/api/twins/{entityType}/{entityId}/model", cors(allowedOrigin, twinmodel.HandleRepoint))

	// ─── Policy catalog (R6) ────────────────────────────────────────────────
	// Tenant-scoped, versioned policy documents (subjects, thing:/... resources,
	// Ditto-style grant/revoke). Method patterns are the executable OpenAPI
	// contract; methodless fallbacks preserve the canonical JSON error envelope.
	mux.HandleFunc("GET /api/policies", cors(allowedOrigin, policy.HandleCollection))
	mux.HandleFunc("POST /api/policies", cors(allowedOrigin, policy.HandleCollection))
	mux.HandleFunc("/api/policies", cors(allowedOrigin, policy.HandleCollection))
	mux.HandleFunc("GET /api/policies/{policyId}/{version}", cors(allowedOrigin, policy.HandleVersion))
	mux.HandleFunc("DELETE /api/policies/{policyId}/{version}", cors(allowedOrigin, policy.HandleVersion))
	mux.HandleFunc("/api/policies/{policyId}/{version}", cors(allowedOrigin, policy.HandleVersion))

	mux.HandleFunc("GET /api/twins", cors(allowedOrigin, policy.EnforceList(twin.HandleList)))
	mux.HandleFunc("/api/twins", cors(allowedOrigin, twin.HandleList))

	mux.HandleFunc("GET /api/twins/{entityType}/{entityId}", cors(allowedOrigin, policy.EnforceRead(twin.PolicyContext, twin.GetByEntity)))

	// Model-validated twin state writes (R3). Method patterns are the
	// executable OpenAPI contract; methodless fallbacks preserve the canonical
	// JSON error envelope on wrong methods. The write routes are wrapped with
	// R6 policy enforcement (authorizes each feature/attribute path against the
	// entity's resolved policy document); the methodless fallbacks are not.
	mux.HandleFunc("PUT /api/twins/{entityType}/{entityId}/attributes", cors(allowedOrigin, policy.EnforceWrite(twin.PolicyContext, twin.HandleSaveAttributes)))
	mux.HandleFunc("PATCH /api/twins/{entityType}/{entityId}/attributes", cors(allowedOrigin, policy.EnforceWrite(twin.PolicyContext, twin.HandleSaveAttributes)))
	mux.HandleFunc("/api/twins/{entityType}/{entityId}/attributes", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		twin.HandleSaveAttributes(w, r, r.PathValue("entityType"), r.PathValue("entityId"))
	}))
	mux.HandleFunc("PUT /api/twins/{entityType}/{entityId}/features", cors(allowedOrigin, policy.EnforceWrite(twin.PolicyContext, twin.HandleSaveFeatures)))
	mux.HandleFunc("PATCH /api/twins/{entityType}/{entityId}/features", cors(allowedOrigin, policy.EnforceWrite(twin.PolicyContext, twin.HandleSaveFeatures)))
	mux.HandleFunc("/api/twins/{entityType}/{entityId}/features", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		twin.HandleSaveFeatures(w, r, r.PathValue("entityType"), r.PathValue("entityId"))
	}))

	// ─── Users list ───────────────────────────────────────────────────────
	mux.HandleFunc("/api/users", cors(allowedOrigin, system.HandleUsersList))
	// UserInfo is a superset of User in TB, but every field the UI reads off it is
	// already in the User payload, so the same listing serves both.
	mux.HandleFunc("/api/users/info", cors(allowedOrigin, system.HandleUsersList))
	// /api/users/assign/{alarmId} — assignee picker for an alarm. The bare
	// form is registered too: without a subtree pattern there is no
	// automatic /api/users/assign → /api/users/assign/ redirect any more.
	mux.HandleFunc("/api/users/assign", cors(allowedOrigin, system.HandleAssignableUsers))
	mux.HandleFunc("/api/users/assign/{alarmId}", cors(allowedOrigin, system.HandleAssignableUsers))

	// ─── Telemetry/Attribute REST API ────────────────────────────────────
	// One dispatcher, registered for every path depth the TB contract uses
	// under /api/plugins/telemetry/{entityType}/{entityId}/… (2–5 segments).
	// The handlers parse entity type/id and scope out of the path
	// themselves, so the patterns only bound the shape.
	pluginsTelemetry := func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		path := r.URL.Path

		// GET .../keys/timeseries → list of telemetry key names
		if strings.HasSuffix(path, "/keys/timeseries") {
			HandleTelemetryKeys(w, r)
			return
		}

		// GET .../values/timeseries → historical data points
		if strings.HasSuffix(path, "/values/timeseries") {
			HandleTelemetryValues(w, r)
			return
		}

		// DELETE .../timeseries/delete — the only path that removes measurements.
		// Disabled by default; see internal/telemetry/delete.go for why.
		if r.Method == http.MethodDelete && strings.HasSuffix(path, "/timeseries/delete") {
			telemetry.HandleDeleteTimeseries(w, r)
			return
		}
		// DELETE .../{scope} removes attributes, not measurements.
		if r.Method == http.MethodDelete {
			tenant.HandleAttributeRest(w, r)
			return
		}

		if r.Method == http.MethodPost && strings.Contains(path, "/timeseries") {
			handleTelemetryRestPost(w, r)
			return
		}

		// GET/POST .../keys/attributes or .../values/attributes
		if strings.Contains(path, "/keys/attributes") || strings.Contains(path, "/values/attributes") || r.Method == http.MethodPost {
			tenant.HandleAttributeRest(w, r)
			return
		}

		// Everything else → proxy
		notImplemented(w, r)
	}
	mux.HandleFunc("/api/plugins/telemetry/{a}/{b}", pluginsTelemetry)
	mux.HandleFunc("/api/plugins/telemetry/{a}/{b}/{c}", pluginsTelemetry)
	mux.HandleFunc("/api/plugins/telemetry/{a}/{b}/{c}/{d}", pluginsTelemetry)
	mux.HandleFunc("/api/plugins/telemetry/{a}/{b}/{c}/{d}/{e}", pluginsTelemetry)

	// ─── Catch-all: any API path the bridge doesn't route explicitly lands
	// here. Two very different cases share this closure and must not share a
	// response:
	//
	//   - Declared no-goals (the contract's 89 out-of-scope endpoints) keep
	//     the forgiving shape — empty PageData on GET — so those screens
	//     render instead of toasting errors.
	//   - Anything else is an endpoint we never decided about. Answering it
	//     with an empty 200 is how 34 endpoints once went silently
	//     unimplemented, so it now fails loudly: 404 for every method.
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		// /api/{ENTITY_TYPE}/{id}/calculatedFields — UI fetches the
		// per-entity CF list when opening a device/asset detail view.
		// We don't materialise calculated fields per entity yet, so an
		// empty page keeps the panel rendering instead of toasting 404.
		path := strings.TrimPrefix(r.URL.Path, "/api/")
		parts := strings.Split(path, "/")
		if len(parts) == 3 && parts[2] == "calculatedFields" {
			system.EmptyPageData(w)
			return
		}
		if isDeclaredNoGoal(r.Method, r.URL.Path) {
			notImplemented(w, r)
			return
		}
		notRouted(w, r)
	})

	// Server-to-device RPC. Tenant isolation and device lookup live in
	// internal/rpc.Handle; this closure only strips the path prefix.
	handleRPC := func(oneway bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Authorization")
			rpc.Handle(w, r, r.PathValue("deviceId"), oneway)
		}
	}
	mux.HandleFunc("POST /api/plugins/rpc/twoway/{deviceId}", handleRPC(false))
	mux.HandleFunc("POST /api/rpc/twoway/{deviceId}", handleRPC(false))
	mux.HandleFunc("POST /api/plugins/rpc/oneway/{deviceId}", handleRPC(true))
	mux.HandleFunc("POST /api/rpc/oneway/{deviceId}", handleRPC(true))
	mux.HandleFunc("/api/rpc/persistent/{id}", cors(allowedOrigin, func(w http.ResponseWriter, r *http.Request) {
		rpc.HandlePersistent(w, r, r.PathValue("id"))
	}))

}

func startHTTPServer() {
	mux := http.NewServeMux()
	allowedOrigin := getEnv("ALLOWED_ORIGIN", "*")
	registerRoutes(mux, allowedOrigin)

	// Deny-by-default authentication gate. It wraps the mux but sits INSIDE
	// logHandler (which sets X-Request-ID, applies the body cap and records
	// the status via statusRecorder). Ordering matters: request-id +
	// access-log wrap the gate, so a rejected 401 still gets a request-id
	// header and is written to the access log — security-relevant, since the
	// 401s are exactly what we want to see. The gate wraps the mux, so an
	// unauthenticated request is rejected before route dispatch and never
	// reaches a handler.
	gatedMux := authGate(mux, allowedOrigin)

	log.Println("Servidor HTTP API escuchando en :8080 (Gateway Mode)")
	// Upload endpoints accept binaries (firmware images, dashboard
	// thumbnails, JS modules). Everything else is JSON metadata and
	// caps at 5MB. Match by prefix because mux dispatch happens later.
	isUploadPath := func(p string) bool {
		return strings.HasPrefix(p, "/api/image/") ||
			strings.HasPrefix(p, "/api/resource") ||
			strings.HasPrefix(p, "/api/otaPackage")
	}
	const (
		defaultMaxBody = 5 << 20   // 5 MB
		uploadMaxBody  = 100 << 20 // 100 MB — matches OTA firmware ceiling
	)
	// Access-log policy: at INFO we elide hot, well-behaved health paths.
	// Set LOG_LEVEL=debug to log every request. Set LOG_ACCESS_SLOW_MS to
	// override the slow-request threshold.
	quietPathPrefixes := []string{
		"/health",
		"/ready",
		"/api/noauth/device-jwks",
		"/api/noauth/oauth2Clients",
		"/.well-known/thingsflow-device-jwks.json",
	}
	slowThresholdMs := 1000
	if v, err := strconv.Atoi(os.Getenv("LOG_ACCESS_SLOW_MS")); err == nil && v > 0 {
		slowThresholdMs = v
	}
	isQuiet := func(p string) bool {
		for _, prefix := range quietPathPrefixes {
			if strings.HasPrefix(p, prefix) {
				return true
			}
		}
		return false
	}
	logHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Honour upstream request-id (k8s ingress, traefik, envoy all
		// emit X-Request-ID); generate one if absent. Echoed in the
		// response and prefixed in our access log so a 500 surfaced in
		// the UI is grep-able to the bridge log line that produced it.
		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			rid = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", rid)
		// Cap request body size to avoid OOM from a malicious large POST.
		max := int64(defaultMaxBody)
		if isUploadPath(r.URL.Path) {
			max = uploadMaxBody
		}
		r.Body = http.MaxBytesReader(w, r.Body, max)

		// SECURITY (H-2): /api/v1/{token}/* embeds the device access
		// token in the URL path. The token never appears in the log.
		safePath := redactDeviceToken(r.URL.Path)
		if r.URL.RawQuery != "" && strings.HasPrefix(r.URL.Path, "/api/widget") {
			safePath = safePath + "?" + r.URL.RawQuery
		}

		started := time.Now()
		ww := &statusRecorder{ResponseWriter: w, status: 200}
		gatedMux.ServeHTTP(ww, r)
		elapsedMs := time.Since(started).Milliseconds()

		// Decide log level + whether to emit at all.
		//   - errors (≥400) and slow requests always log at WARN.
		//   - quiet paths (health/auth/ACL/publish) only log when one
		//     of those triggers fires; ordinary 200s drop to DEBUG.
		//   - everything else logs once at INFO.
		fields := []any{
			slog.String("method", r.Method),
			slog.String("path", safePath),
			slog.Int("status", ww.status),
			slog.Int64("ms", elapsedMs),
			slog.String("rid", rid),
		}
		switch {
		case ww.status >= 500:
			slog.Warn("http", fields...)
		case ww.status == http.StatusNotFound && isExpectedNotFound(r):
			slog.Debug("http", fields...)
		case ww.status >= 400 || elapsedMs >= int64(slowThresholdMs):
			slog.Warn("http", fields...)
		case isQuiet(r.URL.Path):
			slog.Debug("http", fields...)
		default:
			slog.Info("http", fields...)
		}
	})

	// Explicit timeouts close the slowloris vector — clients that open a
	// connection but stall on headers / body / response can't pin worker
	// goroutines indefinitely. ReadHeaderTimeout is the critical one;
	// ReadTimeout / WriteTimeout cap streaming uploads (OTA) and downloads.
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           logHandler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       300 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB header cap
	}

	// Graceful shutdown: when ctx is cancelled (main() on SIGTERM/SIGINT),
	// give in-flight requests up to 30s to finish before yanking sockets.
	// 30s is the kubelet TerminationGracePeriodSeconds default — anything
	// longer gets killed by SIGKILL anyway.
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		log.Println("HTTP server: graceful shutdown starting (30s timeout)")
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("HTTP server: shutdown returned %v", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Error al arrancar servidor HTTP: %v", err)
	}
	log.Println("HTTP server: stopped accepting connections")
}

func isExpectedNotFound(r *http.Request) bool {
	return r.Method == http.MethodGet &&
		r.URL.Path == "/api/tenant/devices" &&
		r.URL.Query().Get("deviceName") != ""
}

func handleTelemetryRestPost(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var entityType, entityID string
	for i, part := range parts {
		if part == "telemetry" && i+2 < len(parts) {
			entityType = parts[i+1]
			entityID = parts[i+2]
			break
		}
	}
	if entityType == "" || entityID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Expected /api/plugins/telemetry/{entityType}/{entityId}/timeseries")
		return
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&payload); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid telemetry payload")
		return
	}

	ts := time.Now().UnixMilli()
	if rawTS, ok := payload["ts"].(float64); ok {
		ts = int64(rawTS)
		if values, ok := payload["values"].(map[string]interface{}); ok {
			payload = values
		}
	}

	if store := twinstore.Global(); store != nil && tenantID != "" {
		if err := store.MergeTelemetry(r.Context(), tenantID, entityType, entityID, ts, payload); err != nil {
			slog.Warn("twin_state_rest_timeseries_write_failed",
				slog.String("tenant_id", tenantID),
				slog.String("entity_type", entityType),
				slog.String("entity_id", entityID),
				slog.Any("error", err),
			)
			httputil.WriteError(w, http.StatusBadGateway, "Twin state write failed")
			return
		}
	}

	// KNOWN DOUBLE-PUSH (deliberately retained this cycle): when a twin store
	// is configured (nats/memory), the MergeTelemetry above also makes the KV
	// watch (twin_state.go) re-broadcast this write — so REST timeseries
	// writes push twice. This pre-dates the single-publisher work (the watch
	// made it visible, it did not introduce it). It is NOT removed here
	// because this direct call is the ONLY push path when store == nil (dev
	// mode / degraded boot), and conditioning it on store presence touches
	// the REST telemetry contract mid-review — outside this cycle's risk
	// budget. Follow-up: make the watch the single telemetry publisher too,
	// mirroring the attribute fix (tracked in
	// .planning/phases/01-foundations-fixes/01-01-SUMMARY.md, Follow-ups).
	ws.BroadcastTelemetry(entityID, payload, ts)
	w.WriteHeader(http.StatusOK)
}

// redactDeviceToken replaces the device access token in
// /api/v1/{token}/{action}[/...] paths with the literal string
// "<redacted>" so log lines don't leak credentials. Other paths pass
// through unchanged. The token segment is the third element after
// the leading slash; preserve everything after it (action + sub-path)
// so "POST /api/v1/<redacted>/telemetry" stays grep-able by action.
func redactDeviceToken(p string) string {
	if p == "/api/v1/provision" {
		return p
	}
	const prefix = "/api/v1/"
	if !strings.HasPrefix(p, prefix) {
		return p
	}
	rest := p[len(prefix):]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		// /api/v1/<token> with no trailing action — still redact.
		return prefix + "<redacted>"
	}
	return prefix + "<redacted>" + rest[slash:]
}

// notImplemented surfaces gaps in the bridge's API coverage. The path
// is always logged at WARN so operators can grep for UI calls that
// need a native handler.
//
// Response shape is method-dependent:
//
//   - GET → 200 with an empty PageData wrapper
//     `{data:[],totalPages:0,totalElements:0,hasNext:false}`. The TB UI
//     v4.x is unforgiving about list endpoints: a 404 with a JSON
//     error body gets parsed by the response interceptor as if it were
//     the success payload, then `.sort()` / iterators run on the error
//     object and crash with "Cannot read properties of undefined" (this
//     is what we hit on /api/widgetsBundles/all before the explicit
//     handler landed). Returning shape-friendly empty data lets the UI
//     render "no results" instead of going kaboom.
//
//   - non-GET → 404 with the structured error body. Mutation calls
//     (POST/PUT/DELETE) where the endpoint genuinely does not exist
//     should fail loud — silent acceptance would mask real bugs.
//
// notRouted answers an API path that is neither implemented nor declared
// out of scope in the UI contract. Unlike notImplemented it refuses the
// empty-200 on GET: an undeclared endpoint answered with an empty page is
// invisible in the UI and in the logs of anyone not grepping for it. The
// distinct log tag is what the contract gate and operators alert on.
func notRouted(w http.ResponseWriter, r *http.Request) {
	slog.Warn("not_routed_undeclared", slog.String("method", r.Method), slog.String("path", redactDeviceToken(r.URL.Path)))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    404,
		"errorCode": 32,
		"message":   "Endpoint not routed and not declared in the UI contract: " + r.Method + " " + r.URL.Path,
		"timestamp": time.Now().UnixMilli(),
	})
}

func notImplemented(w http.ResponseWriter, r *http.Request) {
	slog.Warn("not_implemented", slog.String("method", r.Method), slog.String("path", redactDeviceToken(r.URL.Path)))
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		// Empty PageData — safe shape for any list endpoint the UI calls.
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data":          []interface{}{},
			"totalPages":    0,
			"totalElements": 0,
			"hasNext":       false,
		})
		return
	}
	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    404,
		"errorCode": 32,
		"message":   "Endpoint not implemented in flow-bridge: " + r.Method + " " + r.URL.Path,
		"timestamp": time.Now().UnixMilli(),
	})
}

// setCORSHeaders adds CORS headers to the response.
// setCORSHeaders writes the response CORS headers based on allowedOrigin.
// Spec: Access-Control-Allow-Credentials=true forbids Allow-Origin="*".
// We honour that: if ALLOWED_ORIGIN is "*" we drop the credentials flag,
// which preserves cross-origin GETs from browsers while preventing the
// "all origins can send cookies" footgun. Production deploys should set
// ALLOWED_ORIGIN to the UI's exact origin (https://app.example.com).
func setCORSHeaders(w http.ResponseWriter, allowedOrigin string) {
	w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Authorization, Authorization")
	if allowedOrigin != "*" {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
}

// cors wraps a plain handler with CORS headers + OPTIONS short-circuit.
// Replaces the 5-line inline closure that's repeated ~150x in
// startHTTPServer:
//
//	mux.HandleFunc("/api/x", func(w http.ResponseWriter, r *http.Request) {
//		setCORSHeaders(w, allowedOrigin)
//		if r.Method == "OPTIONS" { w.WriteHeader(http.StatusOK); return }
//		HandleX(w, r)
//	})
//
// becomes:
//
//	mux.HandleFunc("/api/x", cors(allowedOrigin, HandleX))
//
// allowedOrigin is captured by closure so each registration site stays a
// single line. Routes that need extra logic (path-param parsing, method
// dispatch) keep the inline closure form.
func cors(allowedOrigin string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, allowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		h(w, r)
	}
}

// statusRecorder wraps http.ResponseWriter so the access-log middleware
// can read the response status code after the handler returns. We
// default to 200 because Go's net/http only calls WriteHeader when the
// status differs — handlers that just call w.Write() implicitly send 200.
//
// It MUST proxy http.Hijacker, otherwise gorilla/websocket's Upgrade()
// returns "response does not implement http.Hijacker" and every WS
// connection attempt 500s. Same goes for http.Flusher (SSE / streaming
// JSON) and http.CloseNotifier (legacy long-poll). Wrapping is safe
// because the underlying writer always implements them in net/http.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Hijack lets gorilla/websocket take over the TCP connection for the
// upgrade handshake. Without this, every /api/ws request 500s with
// "response does not implement http.Hijacker".
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := s.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("ResponseWriter does not implement http.Hijacker")
}

// Flush keeps SSE / chunked-streaming responses (e.g. live-update
// endpoints) actually flushing through the wrapper.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
