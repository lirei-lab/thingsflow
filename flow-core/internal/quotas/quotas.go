// Package quotas enforces per-tenant limits: counts (devices, assets,
// users, etc.) and ingest rate (transport messages per minute).
//
// Limits live in `tenant_profile.profile_data.configuration` as JSON.
// `LimitsFor(tenantId)` reads that, with a small TTL cache so high-volume
// create paths don't pay a DB round trip per request.
//
// The check-then-act window is wide enough to allow a single
// over-creation under heavy concurrency. We accept that — the goal is
// to keep tenants near their plan, not to enforce a hard cap.
package quotas

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	dbpkg "flow-core/internal/db"
)

// Limits is the JSON shape of the per-tenant configuration block.
// Fields are tagged with the JSON keys TB classic uses so values
// authored in the tenant_profile UI flow through unchanged.
type Limits struct {
	MaxDevices    int64 `json:"maxDevices"`
	MaxAssets     int64 `json:"maxAssets"`
	MaxUsers      int64 `json:"maxUsers"`
	MaxCustomers  int64 `json:"maxCustomers"`
	MaxDashboards int64 `json:"maxDashboards"`
	MaxRuleChains int64 `json:"maxRuleChains"`

	// MaxTransportMessages is the per-minute ceiling on accepted device
	// telemetry/attribute writes. 0 = unlimited (TB classic convention).
	MaxTransportMessages int64 `json:"maxTransportMessages"`
}

type cachedQuota struct {
	limits  Limits
	expires time.Time
}

var (
	cache    = map[string]cachedQuota{}
	cacheMu  sync.RWMutex
	cacheTTL = 30 * time.Second
)

// LimitsFor reads the tenant's active profile and parses limits.
// Cached for cacheTTL; TenantProfile edits propagate within 30s.
func LimitsFor(tenantID string) Limits {
	cacheMu.RLock()
	if c, ok := cache[tenantID]; ok && time.Now().Before(c.expires) {
		cacheMu.RUnlock()
		return c.limits
	}
	cacheMu.RUnlock()

	var profileData []byte
	limits := Limits{}
	if dbpkg.Pool != nil {
		err := dbpkg.Pool.QueryRow(`
			SELECT tp.profile_data::text
			  FROM tenant t
			  JOIN tenant_profile tp ON tp.id = t.tenant_profile_id
			 WHERE t.id = $1`, tenantID,
		).Scan(&profileData)
		if err == nil && len(profileData) > 0 {
			var wrapper struct {
				Configuration Limits `json:"configuration"`
			}
			if jerr := json.Unmarshal(profileData, &wrapper); jerr == nil {
				limits = wrapper.Configuration
			}
		}
	}

	cacheMu.Lock()
	cache[tenantID] = cachedQuota{limits: limits, expires: time.Now().Add(cacheTTL)}
	cacheMu.Unlock()
	return limits
}

// InvalidateCache drops a tenant's cached limits (or all of them when
// tenantID is empty). Called by TenantProfile save/delete paths so
// admins see their changes immediately rather than after the TTL.
func InvalidateCache(tenantID string) {
	cacheMu.Lock()
	if tenantID == "" {
		cache = map[string]cachedQuota{}
	} else {
		delete(cache, tenantID)
	}
	cacheMu.Unlock()
}

// SeedForTest plants a Limits value in the cache with a far-future
// expiry, bypassing the postgres lookup. Tests should call cleanup
// (or InvalidateCache(tenantID)) afterwards to keep the next test
// from seeing leaked state.
func SeedForTest(tenantID string, l Limits) {
	cacheMu.Lock()
	cache[tenantID] = cachedQuota{limits: l, expires: time.Now().Add(time.Hour)}
	cacheMu.Unlock()
}

// spec maps an entity type to its limit field and counting query.
type spec struct {
	limit func(Limits) int64
	table string
	label string
}

var specs = map[string]spec{
	"device":     {func(l Limits) int64 { return l.MaxDevices }, "device", "devices"},
	"asset":      {func(l Limits) int64 { return l.MaxAssets }, "asset", "assets"},
	"user":       {func(l Limits) int64 { return l.MaxUsers }, "tb_user", "users"},
	"customer":   {func(l Limits) int64 { return l.MaxCustomers }, "customer", "customers"},
	"dashboard":  {func(l Limits) int64 { return l.MaxDashboards }, "dashboard", "dashboards"},
	"rule_chain": {func(l Limits) int64 { return l.MaxRuleChains }, "rule_chain", "rule chains"},
}

// Enforce checks whether the tenant can create another entity of the
// given kind. If the answer is no, it writes a 403 and returns false;
// the caller should `return` immediately. 0 = unlimited.
func Enforce(w http.ResponseWriter, tenantID, kind string) bool {
	if dbpkg.Pool == nil || tenantID == "" {
		return true
	}
	s, ok := specs[kind]
	if !ok {
		return true
	}
	limit := s.limit(LimitsFor(tenantID))
	if limit <= 0 {
		return true
	}
	var count int64
	q := fmt.Sprintf(`SELECT count(*) FROM %s WHERE tenant_id = $1`, s.table)
	if err := dbpkg.Pool.QueryRow(q, tenantID).Scan(&count); err != nil {
		log.Printf("WARN Enforce count(%s, %s): %v — allowing through", kind, tenantID, err)
		return true
	}
	if count >= limit {
		http.Error(w,
			fmt.Sprintf("Tenant quota exceeded: limit of %d %s reached for this profile", limit, s.label),
			http.StatusForbidden)
		log.Printf("Quota DENY: tenant=%s kind=%s count=%d limit=%d", tenantID, kind, count, limit)
		return false
	}
	return true
}
