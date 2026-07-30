package bootstrap

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
)

// Classic ThingsBoard's install does TWO things per SCADA symbol SVG: it saves the SVG as
// an IMAGE/SCADA_SYMBOL resource AND it clones the `scada_symbol` widget-type template into
// one widget type per symbol (InstallScripts.saveScadaSymbolWidget). We only did the first,
// so the five SCADA symbol bundles — including "High-performance SCADA energy system" —
// resolved zero widget types and rendered empty in the widget picker.
//
// The bundles reference symbols by FQN (e.g. hp_solar_panel). That FQN is the slugified
// `title` from the SVG's embedded <tb:metadata>; verified to reproduce all 160 bundle FQNs
// exactly for the shipped symbol set.

var (
	scadaMetadataRegex = regexp.MustCompile(`(?s)<tb:metadata[^>]*>(.*?)</tb:metadata>`)
	scadaCDATAOpen     = regexp.MustCompile(`^\s*<!\[CDATA\[`)
	scadaCDATAClose    = regexp.MustCompile(`\]\]>\s*$`)
	scadaNonAlnumRegex = regexp.MustCompile(`[^a-z0-9]+`)
	scadaPreviewWidth  = regexp.MustCompile(`previewWidth: '\d*px'`)
	scadaPreviewHeight = regexp.MustCompile(`previewHeight: '\d*px'`)
)

// scadaSymbolMetadata is the subset of <tb:metadata> the widget type needs.
type scadaSymbolMetadata struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	SearchTags  []string `json:"searchTags"`
	WidgetSizeX float64  `json:"widgetSizeX"`
	WidgetSizeY float64  `json:"widgetSizeY"`
}

// parseScadaSymbolMetadata extracts the <tb:metadata> JSON embedded in a SCADA SVG. The
// payload is wrapped in CDATA, which is not valid JSON, so strip the wrapper first.
func parseScadaSymbolMetadata(svg []byte) (scadaSymbolMetadata, bool) {
	m := scadaMetadataRegex.FindSubmatch(svg)
	if len(m) != 2 {
		return scadaSymbolMetadata{}, false
	}
	body := strings.TrimSpace(string(m[1]))
	body = scadaCDATAOpen.ReplaceAllString(body, "")
	body = scadaCDATAClose.ReplaceAllString(body, "")
	var meta scadaSymbolMetadata
	if err := json.Unmarshal([]byte(strings.TrimSpace(body)), &meta); err != nil {
		return scadaSymbolMetadata{}, false
	}
	if meta.Title == "" {
		return scadaSymbolMetadata{}, false
	}
	if meta.WidgetSizeX <= 0 {
		meta.WidgetSizeX = 1
	}
	if meta.WidgetSizeY <= 0 {
		meta.WidgetSizeY = 1
	}
	return meta, true
}

// scadaSymbolFQN slugifies a symbol title into the FQN the widget bundles reference
// ("HP Solar panel" → "hp_solar_panel").
func scadaSymbolFQN(title string) string {
	s := scadaNonAlnumRegex.ReplaceAllString(strings.ToLower(title), "_")
	return strings.Trim(s, "_")
}

// buildScadaWidgetDescriptor clones the template descriptor and applies the per-symbol
// values, mirroring classic TB: defaultConfig.title, defaultConfig.settings.scadaSymbolUrl,
// sizeX/sizeY and the controllerScript preview dimensions.
func buildScadaWidgetDescriptor(templateDescriptor string, meta scadaSymbolMetadata, symbolURL string) (string, error) {
	var descriptor map[string]interface{}
	if err := json.Unmarshal([]byte(templateDescriptor), &descriptor); err != nil {
		return "", fmt.Errorf("parse scada_symbol template descriptor: %w", err)
	}

	// defaultConfig is stored as a JSON *string* inside the descriptor.
	defaultConfig := map[string]interface{}{}
	if raw, ok := descriptor["defaultConfig"].(string); ok && raw != "" {
		_ = json.Unmarshal([]byte(raw), &defaultConfig)
	}
	defaultConfig["title"] = meta.Title
	settings, _ := defaultConfig["settings"].(map[string]interface{})
	if settings == nil {
		settings = map[string]interface{}{}
	}
	settings["scadaSymbolUrl"] = symbolURL
	defaultConfig["settings"] = settings
	encoded, err := json.Marshal(defaultConfig)
	if err != nil {
		return "", fmt.Errorf("encode defaultConfig: %w", err)
	}
	descriptor["defaultConfig"] = string(encoded)
	descriptor["sizeX"] = meta.WidgetSizeX
	descriptor["sizeY"] = meta.WidgetSizeY

	if script, ok := descriptor["controllerScript"].(string); ok {
		script = scadaPreviewWidth.ReplaceAllString(script,
			fmt.Sprintf("previewWidth: '%dpx'", int(meta.WidgetSizeX*100)))
		script = scadaPreviewHeight.ReplaceAllString(script,
			fmt.Sprintf("previewHeight: '%dpx'", int(meta.WidgetSizeY*100)+20))
		descriptor["controllerScript"] = script
	}

	out, err := json.Marshal(descriptor)
	if err != nil {
		return "", fmt.Errorf("encode descriptor: %w", err)
	}
	return string(out), nil
}

// loadScadaSymbolWidgets creates one widget type per shipped SCADA symbol from the
// `scada_symbol` template, then links them into the bundles that reference them.
// Idempotent: skips entirely once the per-symbol widget types exist.
func loadScadaSymbolWidgets(root string) error {
	var existing int
	if err := dbpkg.Pool.QueryRow(
		`SELECT count(*) FROM widget_type WHERE tenant_id = $1 AND scada = true AND fqn <> 'scada_symbol'`,
		systemTenantId,
	).Scan(&existing); err != nil {
		return fmt.Errorf("count scada widget types: %w", err)
	}
	if existing > 0 {
		log.Printf("scada symbol widgets: %d already seeded — skipping", existing)
		return nil
	}

	var templateDescriptor string
	err := dbpkg.Pool.QueryRow(
		`SELECT descriptor FROM widget_type WHERE tenant_id = $1 AND fqn = 'scada_symbol'`,
		systemTenantId,
	).Scan(&templateDescriptor)
	if err != nil {
		// Without the template there is nothing to clone; the symbols themselves are still
		// available as images, so this is a skip rather than a hard failure.
		log.Printf("scada symbol widgets: template 'scada_symbol' not found — skipping (%v)", err)
		return nil
	}

	files, _ := filepath.Glob(filepath.Join(root, "scada_symbols", "*.svg"))
	if len(files) == 0 {
		log.Printf("scada symbol widgets: no .svg files — skipping")
		return nil
	}

	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		return fmt.Errorf("tx begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UnixMilli()
	fqnToID := map[string]string{}
	inserted, skipped := 0, 0
	for _, f := range files {
		svg, err := os.ReadFile(f)
		if err != nil {
			log.Printf("WARN read %s: %v", filepath.Base(f), err)
			skipped++
			continue
		}
		meta, ok := parseScadaSymbolMetadata(svg)
		if !ok {
			log.Printf("WARN scada symbol %s: no usable <tb:metadata> — skipped", filepath.Base(f))
			skipped++
			continue
		}
		fqn := scadaSymbolFQN(meta.Title)
		if fqn == "" || fqn == "scada_symbol" {
			skipped++
			continue
		}
		symbolURL := "tb-image;/api/images/system/" + filepath.Base(f)
		descriptor, err := buildScadaWidgetDescriptor(templateDescriptor, meta, symbolURL)
		if err != nil {
			log.Printf("WARN scada symbol %s: %v", filepath.Base(f), err)
			skipped++
			continue
		}
		id := uuid.New().String()
		if _, err := tx.Exec(`
			INSERT INTO widget_type (id, created_time, tenant_id, fqn, name, image,
			                          deprecated, description, descriptor, tags, scada)
			VALUES ($1, $2, $3, $4, $5, $6, false, NULLIF($7,''), $8, $9, true)
			ON CONFLICT (tenant_id, fqn) DO NOTHING`,
			id, now, systemTenantId, fqn, meta.Title, symbolURL,
			meta.Description, descriptor, pgArray(meta.SearchTags),
		); err != nil {
			log.Printf("WARN insert scada widget_type %s: %v", fqn, err)
			skipped++
			continue
		}
		fqnToID[fqn] = id
		inserted++
	}

	linked := linkBundlesToScadaWidgets(tx, root, fqnToID)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("tx commit: %w", err)
	}
	log.Printf("scada symbol widgets: seeded %d (skipped %d), linked %d bundle entries", inserted, skipped, linked)
	return nil
}

// linkBundlesToScadaWidgets adds widgets_bundle_widget rows for every bundle FQN that now
// resolves to one of the freshly created SCADA widget types. Order follows the bundle JSON.
func linkBundlesToScadaWidgets(tx *sql.Tx, root string, fqnToID map[string]string) int {
	if len(fqnToID) == 0 {
		return 0
	}
	bundleFiles, _ := filepath.Glob(filepath.Join(root, "widget_bundles", "*.json"))
	linked := 0
	for _, f := range bundleFiles {
		buf, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var b widgetBundleJSON
		if err := json.Unmarshal(buf, &b); err != nil {
			continue
		}
		alias := b.WidgetsBundle.Alias
		if alias == "" {
			continue
		}
		var bundleID string
		if err := tx.QueryRow(
			`SELECT id FROM widgets_bundle WHERE tenant_id = $1 AND alias = $2`,
			systemTenantId, alias,
		).Scan(&bundleID); err != nil {
			continue
		}
		for i, fqn := range b.WidgetTypeFqns {
			wtID, ok := fqnToID[fqn]
			if !ok {
				continue
			}
			if _, err := tx.Exec(`
				INSERT INTO widgets_bundle_widget (widgets_bundle_id, widget_type_id, widget_type_order)
				VALUES ($1, $2, $3)
				ON CONFLICT (widgets_bundle_id, widget_type_id) DO NOTHING`,
				bundleID, wtID, i,
			); err != nil {
				log.Printf("WARN link bundle %s ↔ %s: %v", alias, fqn, err)
				continue
			}
			linked++
		}
	}
	return linked
}
