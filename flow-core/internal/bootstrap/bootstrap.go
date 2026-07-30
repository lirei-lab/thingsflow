// Package bootstrap seeds the system catalogue (widget bundles +
// widget types + system dashboards + SCADA symbols + system images +
// OAuth templates + demo dashboards) into postgres on first start. Idempotent — every loader
// no-ops when its target table is already populated, so the function
// is safe to call on every bridge boot.
package bootstrap

// LoadSystemBootstrap seeds the system-wide catalogue (widget types, widget
// bundles and default dashboards) from the JSON files baked into the image at
// /etc/flow/tb-resources. This runs only when the relevant
// table is empty — running it on a populated DB is a no-op.
//
// Mirrors what TB classic does in DefaultSystemDataLoaderService at install
// time, packaged into our Go startup so the bridge converges to a fully
// usable UI without an additional Job.

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
)

// SystemTenantID is TB's well-known UUID for system-level resources.
// Exported so package-main code can reuse it without re-declaring.
const SystemTenantID = "13814000-1dd2-11b2-8080-808080808080"

// systemTenantId is an unexported alias kept so the rest of the file
// (which uses the lowercase name) needs no further changes.
const systemTenantId = SystemTenantID

// getEnv mirrors the helper in package main (kept private for bootstrap paths).
func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// LoadSystemBootstrap orchestrates all the system data loaders. Each step is
// independent and idempotent (skips if its target table is already seeded),
// so it's safe to run on every bridge start.
func LoadSystem() {
	if dbpkg.Pool == nil {
		log.Println("WARN: dbpkg.Pool nil — skipping system bootstrap")
		return
	}
	root := getEnv("TB_RESOURCES_DIR", "/etc/flow/tb-resources")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		log.Printf("WARN: tb-resources not present at %s — skipping system bootstrap", root)
		return
	}

	if err := loadSystemWidgets(root); err != nil {
		log.Printf("WARN: loadSystemWidgets failed: %v", err)
	}
	if err := loadSystemDashboards(root); err != nil {
		log.Printf("WARN: loadSystemDashboards failed: %v", err)
	}
	if err := loadScadaSymbols(root); err != nil {
		log.Printf("WARN: loadScadaSymbols failed: %v", err)
	}
	// Must run AFTER loadSystemWidgets (needs the `scada_symbol` template widget type and
	// the bundle rows to link into) and after loadScadaSymbols (the symbol images it points at).
	if err := loadScadaSymbolWidgets(root); err != nil {
		log.Printf("WARN: loadScadaSymbolWidgets failed: %v", err)
	}
	if err := loadSystemImages(root); err != nil {
		log.Printf("WARN: loadSystemImages failed: %v", err)
	}
	if err := loadOAuth2Templates(root); err != nil {
		log.Printf("WARN: loadOAuth2Templates failed: %v", err)
	}
	if err := loadDemoDashboards(root); err != nil {
		log.Printf("WARN: loadDemoDashboards failed: %v", err)
	}
}

// --- OAuth2 templates ------------------------------------------------------

// oauth2TemplateJSON mirrors the on-disk shape from
// application/src/main/data/json/system/oauth2_config_templates/. We only
// pull out the columns the DB cares about — everything else is config the
// admin can tweak when materialising a real client.
type oauth2TemplateJSON struct {
	ProviderID                 string   `json:"providerId"`
	AccessTokenURI             string   `json:"accessTokenUri"`
	AuthorizationURI           string   `json:"authorizationUri"`
	Scope                      []string `json:"scope"`
	JwkSetURI                  string   `json:"jwkSetUri"`
	UserInfoURI                string   `json:"userInfoUri"`
	ClientAuthenticationMethod string   `json:"clientAuthenticationMethod"`
	UserNameAttributeName      string   `json:"userNameAttributeName"`
	Comment                    string   `json:"comment"`
	LoginButtonIcon            string   `json:"loginButtonIcon"`
	LoginButtonLabel           string   `json:"loginButtonLabel"`
	HelpLink                   string   `json:"helpLink"`
	MapperConfig               struct {
		Type  string `json:"type"`
		Basic struct {
			EmailAttributeKey     string `json:"emailAttributeKey"`
			FirstNameAttributeKey string `json:"firstNameAttributeKey"`
			LastNameAttributeKey  string `json:"lastNameAttributeKey"`
			TenantNameStrategy    string `json:"tenantNameStrategy"`
		} `json:"basic"`
	} `json:"mapperConfig"`
}

// loadOAuth2Templates seeds tb-resources/oauth2_templates/*.json into
// `oauth2_client_registration_template`. Idempotent via the
// `provider_id` UNIQUE constraint — re-running on a populated table
// skips existing rows.
func loadOAuth2Templates(root string) error {
	var existing int
	if err := dbpkg.Pool.QueryRow(`SELECT count(*) FROM oauth2_client_registration_template`).Scan(&existing); err != nil {
		return fmt.Errorf("count templates: %w", err)
	}
	if existing > 0 {
		log.Printf("oauth2 templates: %d already seeded — skipping", existing)
		return nil
	}
	dir := filepath.Join(root, "oauth2_templates")
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	if len(files) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	inserted := 0
	for _, f := range files {
		buf, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var t oauth2TemplateJSON
		if err := json.Unmarshal(buf, &t); err != nil {
			log.Printf("WARN parse oauth2 template %s: %v", filepath.Base(f), err)
			continue
		}
		_, err = dbpkg.Pool.Exec(`
			INSERT INTO oauth2_client_registration_template (
			    id, created_time, provider_id,
			    authorization_uri, token_uri, scope, user_info_uri,
			    user_name_attribute_name, jwk_set_uri,
			    client_authentication_method, type,
			    basic_email_attribute_key, basic_first_name_attribute_key,
			    basic_last_name_attribute_key, basic_tenant_name_strategy,
			    comment, login_button_icon, login_button_label, help_link
			) VALUES (
			    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			    $12, $13, $14, $15, $16, $17, $18, $19
			) ON CONFLICT (provider_id) DO NOTHING`,
			uuid.New().String(), now, t.ProviderID,
			t.AuthorizationURI, t.AccessTokenURI, strings.Join(t.Scope, ","), t.UserInfoURI,
			t.UserNameAttributeName, t.JwkSetURI,
			t.ClientAuthenticationMethod, t.MapperConfig.Type,
			t.MapperConfig.Basic.EmailAttributeKey, t.MapperConfig.Basic.FirstNameAttributeKey,
			t.MapperConfig.Basic.LastNameAttributeKey, t.MapperConfig.Basic.TenantNameStrategy,
			t.Comment, t.LoginButtonIcon, t.LoginButtonLabel, t.HelpLink,
		)
		if err != nil {
			log.Printf("WARN insert oauth2 template %s: %v", t.ProviderID, err)
			continue
		}
		inserted++
	}
	log.Printf("oauth2 templates: seeded %d/%d providers", inserted, len(files))
	return nil
}

// --- Demo dashboards -------------------------------------------------------

// loadDemoDashboards seeds tb-resources/demo_dashboards/*.json into the
// default tenant so freshly logged-in tenants see something on the
// dashboard list. Idempotent per title so newly added bundled dashboards
// are inserted on upgrade without overwriting operator-created dashboards.
func loadDemoDashboards(root string) error {
	const defaultTenantId = "aaaaaaaa-1dd2-11b2-8080-808080808080"
	dir := filepath.Join(root, "demo_dashboards")
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	if len(files) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	inserted := 0
	for _, f := range files {
		buf, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var d struct {
			Title         string          `json:"title"`
			Configuration json.RawMessage `json:"configuration"`
			Image         string          `json:"image"`
		}
		if err := json.Unmarshal(buf, &d); err != nil {
			log.Printf("WARN parse demo dashboard %s: %v", filepath.Base(f), err)
			continue
		}
		if d.Title == "" {
			d.Title = strings.TrimSuffix(filepath.Base(f), ".json")
		}
		var existingID string
		err = dbpkg.Pool.QueryRow(
			`SELECT id::text FROM dashboard WHERE tenant_id = $1 AND title = $2 LIMIT 1`,
			defaultTenantId, d.Title,
		).Scan(&existingID)
		if err == nil {
			_, err = dbpkg.Pool.Exec(`
				UPDATE dashboard
				SET image = NULLIF($1,''), configuration = $2, version = COALESCE(version, 0) + 1
				WHERE id = $3`,
				d.Image, string(d.Configuration), existingID,
			)
			if err != nil {
				log.Printf("WARN update demo dashboard %s: %v", d.Title, err)
			}
			continue
		}
		_, err = dbpkg.Pool.Exec(`
			INSERT INTO dashboard (id, created_time, tenant_id, title, image, configuration)
			VALUES ($1, $2, $3, $4, NULLIF($5,''), $6)`,
			uuid.New().String(), now, defaultTenantId, d.Title, d.Image, string(d.Configuration),
		)
		if err != nil {
			log.Printf("WARN insert demo dashboard %s: %v", d.Title, err)
			continue
		}
		inserted++
	}
	log.Printf("demo dashboards: seeded %d/%d for default tenant", inserted, len(files))
	return nil
}

// --- SCADA symbols ---------------------------------------------------------

// SVG attribute regexes for the ImageDescriptor's width/height. We
// accept the attrs in either order; viewBox is used as a fallback when
// the root <svg> doesn't declare width/height directly (~10% of TB's
// shipped symbols).
var (
	scadaSvgWidthRegex  = regexp.MustCompile(`<svg\b[^>]*\bwidth="([\d.]+)"`)
	scadaSvgHeightRegex = regexp.MustCompile(`<svg\b[^>]*\bheight="([\d.]+)"`)
	scadaViewBoxRegex   = regexp.MustCompile(`<svg\b[^>]*\bviewBox="[\d.]+\s+[\d.]+\s+([\d.]+)\s+([\d.]+)"`)
)

// buildImageDescriptor builds the JSON the UI's Resource Info row
// consumes. Schema mirrors TB classic's ImageDescriptor:
//
//	{
//	  "mediaType": "image/svg+xml",
//	  "width": 400,
//	  "height": 400,
//	  "size": 14991,
//	  "etag": "..."
//	}
//
// We do NOT cram the SCADA <tb:metadata> JSON in here — the UI parses
// that out of the SVG file itself when it loads a symbol. Mixing the
// two confuses the row renderer and the gallery shows blank rows.
func buildImageDescriptor(svg []byte, etag string) string {
	d := map[string]interface{}{
		"mediaType": "image/svg+xml",
		"size":      len(svg),
		"etag":      etag,
	}
	if m := scadaSvgWidthRegex.FindSubmatch(svg); len(m) == 2 {
		if v, err := strconv.ParseFloat(string(m[1]), 64); err == nil {
			d["width"] = int(v)
		}
	}
	if m := scadaSvgHeightRegex.FindSubmatch(svg); len(m) == 2 {
		if v, err := strconv.ParseFloat(string(m[1]), 64); err == nil {
			d["height"] = int(v)
		}
	}
	if _, hasW := d["width"]; !hasW {
		if m := scadaViewBoxRegex.FindSubmatch(svg); len(m) == 3 {
			if w, err := strconv.ParseFloat(string(m[1]), 64); err == nil {
				d["width"] = int(w)
			}
			if h, err := strconv.ParseFloat(string(m[2]), 64); err == nil {
				d["height"] = int(h)
			}
		}
	}
	out, _ := json.Marshal(d)
	return string(out)
}

// loadScadaSymbols seeds tb-resources/scada_symbols/*.svg into the
// `resource` table as system-scoped rows. Idempotent: when the table
// already has system SCADA symbols, the function is a no-op.
//
// Storage convention (matches TB classic and what the UI filters by):
//
//	resource_type     = 'IMAGE'           — top-level category
//	resource_sub_type = 'SCADA_SYMBOL'    — disambiguates the SCADA gallery
//
// File naming convention mirrors TB classic's seed: the filename minus
// the .svg becomes the resource_key; the title is a humanised version
// (kebab-case → Title Case).
//
// We parse <tb:metadata> from each SVG and store it as `descriptor`
// JSON so the UI knows how to preview, animate, and behave the symbol.
func loadScadaSymbols(root string) error {
	var existing int
	if err := dbpkg.Pool.QueryRow(
		`SELECT count(*) FROM resource
		  WHERE tenant_id = $1
		    AND resource_type = 'IMAGE'
		    AND resource_sub_type = 'SCADA_SYMBOL'`,
		systemTenantId,
	).Scan(&existing); err != nil {
		return fmt.Errorf("count resource: %w", err)
	}
	if existing > 0 {
		log.Printf("system scada symbols: %d already seeded — skipping", existing)
		return nil
	}

	dir := filepath.Join(root, "scada_symbols")
	files, _ := filepath.Glob(filepath.Join(dir, "*.svg"))
	if len(files) == 0 {
		log.Printf("system scada symbols: no .svg files at %s — skipping", dir)
		return nil
	}

	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		return fmt.Errorf("tx begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UnixMilli()
	inserted := 0
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			log.Printf("WARN read %s: %v", f, err)
			continue
		}
		base := strings.TrimSuffix(filepath.Base(f), ".svg")
		title := humaniseKebab(base)
		etag := fmt.Sprintf("%x", len(data))
		descriptor := buildImageDescriptor(data, etag)
		_, err = tx.Exec(`
			INSERT INTO resource (id, created_time, tenant_id, title,
			                     resource_type, resource_sub_type,
			                     resource_key, search_text,
			                     file_name, data, etag, descriptor, is_public)
			VALUES ($1, $2, $3, $4, 'IMAGE', 'SCADA_SYMBOL', $5, $6, $7, $8, $9, $10, true)
			ON CONFLICT (tenant_id, resource_type, resource_key) DO NOTHING`,
			uuid.New().String(), now, systemTenantId, title,
			base+".svg", strings.ToLower(title), base+".svg", data,
			etag, descriptor,
		)
		if err != nil {
			log.Printf("WARN insert scada %s: %v", base, err)
			continue
		}
		inserted++
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("tx commit: %w", err)
	}
	log.Printf("system scada symbols: seeded %d/%d files", inserted, len(files))
	return nil
}

// loadSystemImages seeds tb-resources/system_images/*.{svg,png} into the
// `resource` table as system-scoped IMAGE rows. These back the
// `/api/images/system/<key>` URLs that widget_type and widgets_bundle JSONs
// reference for their picker thumbnails. Without them the widget picker
// shows "No image preview" for every chart/card/etc.
//
// The bytes are produced by tools/python/extract-system-images.py — see that
// script for how we walk git history to recover what's been stripped from
// the working tree. (Some keys are never recoverable from OSS source — they
// only exist in TB's pre-seeded Docker image and we accept the gap.)
//
// resource_type='IMAGE', resource_sub_type='IMAGE' (vs SCADA_SYMBOL which
// the SCADA gallery filters by). No early-return guard — subsequent boots
// re-walk the dir so newly-extracted keys land without a DB wipe; existing
// rows are protected by ON CONFLICT DO NOTHING.
func loadSystemImages(root string) error {
	dir := filepath.Join(root, "system_images")
	manifestPath := filepath.Join(dir, "_manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		log.Printf("system images: manifest not present at %s — skipping", manifestPath)
		return nil
	}
	var manifest map[string]struct {
		Title     string `json:"title"`
		MediaType string `json:"mediaType"`
		Etag      string `json:"etag"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		return fmt.Errorf("tx begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UnixMilli()
	inserted := 0
	for key, meta := range manifest {
		data, err := os.ReadFile(filepath.Join(dir, key))
		if err != nil {
			log.Printf("WARN read system image %s: %v", key, err)
			continue
		}
		title := meta.Title
		if title == "" {
			title = key
		}
		etag := meta.Etag
		if etag == "" {
			etag = fmt.Sprintf("%x", len(data))
		}
		descriptor := buildSystemImageDescriptor(data, meta.MediaType, etag)
		_, err = tx.Exec(`
			INSERT INTO resource (id, created_time, tenant_id, title,
			                     resource_type, resource_sub_type,
			                     resource_key, search_text,
			                     file_name, data, etag, descriptor, is_public)
			VALUES ($1, $2, $3, $4, 'IMAGE', 'IMAGE', $5, $6, $7, $8, $9, $10, true)
			ON CONFLICT (tenant_id, resource_type, resource_key) DO NOTHING`,
			uuid.New().String(), now, systemTenantId, title,
			key, strings.ToLower(title), key, data, etag, descriptor,
		)
		if err != nil {
			log.Printf("WARN insert system image %s: %v", key, err)
			continue
		}
		inserted++
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("tx commit: %w", err)
	}
	log.Printf("system images: seeded %d/%d entries", inserted, len(manifest))
	return nil
}

// buildSystemImageDescriptor produces the descriptor JSON for the resource
// row. SVG dimensions are sniffed from the markup; PNG/JPEG have no cheap
// way to extract pixel size from Go stdlib without decoding, so we leave
// width/height absent — the UI handles that gracefully.
func buildSystemImageDescriptor(data []byte, mediaType, etag string) string {
	d := map[string]interface{}{
		"mediaType": mediaType,
		"size":      len(data),
		"etag":      etag,
	}
	if mediaType == "image/svg+xml" {
		if m := scadaSvgWidthRegex.FindSubmatch(data); len(m) == 2 {
			if v, err := strconv.ParseFloat(string(m[1]), 64); err == nil {
				d["width"] = int(v)
			}
		}
		if m := scadaSvgHeightRegex.FindSubmatch(data); len(m) == 2 {
			if v, err := strconv.ParseFloat(string(m[1]), 64); err == nil {
				d["height"] = int(v)
			}
		}
		if _, hasW := d["width"]; !hasW {
			if m := scadaViewBoxRegex.FindSubmatch(data); len(m) == 3 {
				if w, err := strconv.ParseFloat(string(m[1]), 64); err == nil {
					d["width"] = int(w)
				}
				if h, err := strconv.ParseFloat(string(m[2]), 64); err == nil {
					d["height"] = int(h)
				}
			}
		}
	}
	out, _ := json.Marshal(d)
	return string(out)
}

// humaniseKebab turns "3-phase-voltage-relay-hp" into
// "3 Phase Voltage Relay Hp" — close enough to TB classic's titles for
// the SCADA gallery to be navigable.
func humaniseKebab(s string) string {
	parts := strings.Split(s, "-")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

// --- widgets ---------------------------------------------------------------

type widgetTypeJSON struct {
	FQN         string          `json:"fqn"`
	Name        string          `json:"name"`
	Deprecated  bool            `json:"deprecated"`
	Image       string          `json:"image"`
	Description string          `json:"description"`
	Descriptor  json.RawMessage `json:"descriptor"`
	Tags        []string        `json:"tags"`
	Resources   json.RawMessage `json:"resources"`
}

type widgetBundleJSON struct {
	WidgetsBundle struct {
		Alias       string `json:"alias"`
		Title       string `json:"title"`
		Image       string `json:"image"`
		Description string `json:"description"`
		Order       int    `json:"order"`
		Name        string `json:"name"`
	} `json:"widgetsBundle"`
	WidgetTypeFqns []string `json:"widgetTypeFqns"`
}

func loadSystemWidgets(root string) error {
	var n int
	if err := dbpkg.Pool.QueryRow("SELECT count(*) FROM widget_type WHERE tenant_id = $1", systemTenantId).Scan(&n); err != nil {
		return fmt.Errorf("count widget_type: %w", err)
	}
	if n > 0 {
		log.Printf("system widgets: %d types already seeded — skipping", n)
		return nil
	}

	typesDir := filepath.Join(root, "widget_types")
	bundlesDir := filepath.Join(root, "widget_bundles")

	// 1. INSERT widget_type rows. fqn → uuid map for the bundle linker.
	fqnToId := map[string]string{}
	files, _ := filepath.Glob(filepath.Join(typesDir, "*.json"))
	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		return fmt.Errorf("tx begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UnixMilli()
	for _, f := range files {
		buf, err := os.ReadFile(f)
		if err != nil {
			log.Printf("WARN read %s: %v", f, err)
			continue
		}
		var wt widgetTypeJSON
		if err := json.Unmarshal(buf, &wt); err != nil {
			log.Printf("WARN parse %s: %v", filepath.Base(f), err)
			continue
		}
		if wt.FQN == "" {
			continue
		}
		id := uuid.New().String()
		// Image and descriptor go into character varying(1000000) — large but capped.
		_, err = tx.Exec(`
			INSERT INTO widget_type (id, created_time, tenant_id, fqn, name, image,
			                          deprecated, description, descriptor, tags, scada)
			VALUES ($1, $2, $3, $4, $5, NULLIF($6,''), $7, NULLIF($8,''), $9, $10, false)
			ON CONFLICT (tenant_id, fqn) DO NOTHING`,
			id, now, systemTenantId, wt.FQN, wt.Name, rewriteInlineTbImage(wt.Image),
			wt.Deprecated, wt.Description, string(wt.Descriptor), pgArray(wt.Tags),
		)
		if err != nil {
			log.Printf("WARN insert widget_type %s: %v", wt.FQN, err)
			continue
		}
		fqnToId[wt.FQN] = id
	}

	// 2. INSERT widgets_bundle rows + widgets_bundle_widget links.
	bundleFiles, _ := filepath.Glob(filepath.Join(bundlesDir, "*.json"))
	for _, f := range bundleFiles {
		buf, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var b widgetBundleJSON
		if err := json.Unmarshal(buf, &b); err != nil {
			log.Printf("WARN parse bundle %s: %v", filepath.Base(f), err)
			continue
		}
		bundleId := uuid.New().String()
		_, err = tx.Exec(`
			INSERT INTO widgets_bundle (id, created_time, tenant_id, alias, title,
			                            image, description, widgets_bundle_order, scada)
			VALUES ($1, $2, $3, $4, $5, NULLIF($6,''), NULLIF($7,''), $8, false)
			ON CONFLICT DO NOTHING`,
			bundleId, now, systemTenantId, b.WidgetsBundle.Alias, b.WidgetsBundle.Title,
			rewriteInlineTbImage(b.WidgetsBundle.Image), b.WidgetsBundle.Description, b.WidgetsBundle.Order,
		)
		if err != nil {
			log.Printf("WARN insert widgets_bundle %s: %v", b.WidgetsBundle.Alias, err)
			continue
		}
		for i, fqn := range b.WidgetTypeFqns {
			wtId, ok := fqnToId[fqn]
			if !ok {
				// Reference to widget_type not present in our catalogue;
				// happens when bundles reference deprecated/PRO widgets.
				continue
			}
			if _, err := tx.Exec(`
				INSERT INTO widgets_bundle_widget (widgets_bundle_id, widget_type_id, widget_type_order)
				VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				bundleId, wtId, i,
			); err != nil {
				log.Printf("WARN link bundle %s ↔ %s: %v", b.WidgetsBundle.Alias, fqn, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("tx commit: %w", err)
	}
	log.Printf("system widgets: loaded %d types and %d bundles from %s",
		len(fqnToId), len(bundleFiles), root)
	return nil
}

// rewriteInlineTbImage converts the inline form
//
//	tb-image:<base64-key>:<base64-name>[:<base64-subtype>[:<etag>]];data:<media>;base64,<payload>
//
// into the URL-ref form
//
//	tb-image;/api/images/system/<decoded-key>
//
// which is what the UI actually renders. Bundle JSONs in this repo still
// ship the inline form for their thumbnails; if we store that in
// widgets_bundle.image as-is, the picker shows broken images. The bytes
// themselves are already extracted into the resource table by
// loadSystemImages, so we only need to swap the column value to a ref.
//
// Anything not starting with "tb-image:" passes through unchanged
// (raw "data:image/..." URLs render fine on their own; "tb-image;..." refs
// are already in the right shape).
func rewriteInlineTbImage(s string) string {
	const prefix = "tb-image:"
	if !strings.HasPrefix(s, prefix) {
		return s
	}
	rest := s[len(prefix):]
	semi := strings.Index(rest, ";")
	if semi < 0 {
		return s
	}
	first := rest[:semi]
	colon := strings.Index(first, ":")
	keyB64 := first
	if colon >= 0 {
		keyB64 = first[:colon]
	}
	if keyB64 == "" {
		return s
	}
	keyBytes, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil {
		return s
	}
	return "tb-image;/api/images/system/" + string(keyBytes)
}

// pgArray converts a Go []string to a Postgres `text[]` literal.
// (lib/pq's array support requires importing lib/pq.Array but we keep it
// minimal here and inline the array literal.)
func pgArray(values []string) interface{} {
	if len(values) == 0 {
		return nil
	}
	parts := make([]string, 0, len(values))
	for _, v := range values {
		// Escape backslashes and double quotes inside array elements.
		v = strings.ReplaceAll(v, `\`, `\\`)
		v = strings.ReplaceAll(v, `"`, `\"`)
		parts = append(parts, `"`+v+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// --- system dashboards ----------------------------------------------------

func loadSystemDashboards(root string) error {
	dashDir := filepath.Join(root, "dashboards")
	files, _ := filepath.Glob(filepath.Join(dashDir, "*.json"))
	if len(files) == 0 {
		return nil
	}

	now := time.Now().UnixMilli()
	for _, f := range files {
		buf, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var d struct {
			Title         string          `json:"title"`
			Configuration json.RawMessage `json:"configuration"`
			Image         string          `json:"image"`
		}
		if err := json.Unmarshal(buf, &d); err != nil {
			log.Printf("WARN parse dashboard %s: %v", filepath.Base(f), err)
			continue
		}
		if d.Title == "" {
			d.Title = strings.TrimSuffix(filepath.Base(f), ".json")
		}
		// Skip if a system dashboard with the same title already exists.
		var existing string
		err = dbpkg.Pool.QueryRow(
			"SELECT id::text FROM dashboard WHERE tenant_id = $1 AND title = $2 LIMIT 1",
			systemTenantId, d.Title,
		).Scan(&existing)
		if err == nil {
			continue // already seeded
		}
		if err != sql.ErrNoRows {
			log.Printf("WARN dashboard probe %s: %v", d.Title, err)
			continue
		}
		id := uuid.New().String()
		_, err = dbpkg.Pool.Exec(`
			INSERT INTO dashboard (id, created_time, tenant_id, title, image, configuration)
			VALUES ($1, $2, $3, $4, NULLIF($5,''), $6)`,
			id, now, systemTenantId, d.Title, d.Image, string(d.Configuration),
		)
		if err != nil {
			log.Printf("WARN insert dashboard %s: %v", d.Title, err)
			continue
		}
		log.Printf("system dashboard loaded: %s (id=%s)", d.Title, id)
	}
	return nil
}
