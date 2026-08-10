package system

import (
	"database/sql"
	"os"
	"reflect"
	"testing"

	_ "github.com/lib/pq"

	dbpkg "flow-core/internal/db"
)

// Coverage for the sidebar-menu fix: the TB UI reads
// userSettings.openedMenuSections and crashes (reading 'includes' on
// undefined) when the backend returns a GENERAL setting that lacks the key.
// loadUserSettings must always merge the default openedMenuSections into
// whatever was stored, including for an empty {} row or no row at all.

func newUserSettingsDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	stmts := []string{
		`DROP TABLE IF EXISTS user_settings CASCADE`,
		`CREATE TABLE user_settings (user_id text NOT NULL, type text NOT NULL, settings jsonb, PRIMARY KEY (user_id, type))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		db.Close()
	})
	return db
}

func TestLoadUserSettingsMergesOpenedMenuSections(t *testing.T) {
	db := newUserSettingsDB(t)

	// A GENERAL setting stored as an empty {} (e.g. written by an older
	// flow-core or a partial PUT) must still expose openedMenuSections.
	if _, err := db.Exec(
		`INSERT INTO user_settings (user_id, type, settings) VALUES ($1, 'GENERAL', '{}'::jsonb)`,
		"bbbbbbbb-2222-3333-4444-555555555555"); err != nil {
		t.Fatalf("seed empty: %v", err)
	}
	got := loadUserSettings("bbbbbbbb-2222-3333-4444-555555555555")
	want := []string{"/entities"}
	opened, ok := got["openedMenuSections"]
	if !ok {
		t.Fatalf("loadUserSettings returned %#v without openedMenuSections", got)
	}
	if !reflect.DeepEqual(opened, want) {
		t.Fatalf("openedMenuSections = %#v, want %#v", opened, want)
	}
}

func TestLoadUserSettingsPreservesExistingOpenedMenuSections(t *testing.T) {
	db := newUserSettingsDB(t)

	if _, err := db.Exec(
		`INSERT INTO user_settings (user_id, type, settings) VALUES ($1, 'GENERAL', '{"openedMenuSections":["/devices"]}'::jsonb)`,
		"cccccccc-2222-3333-4444-555555555555"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got := loadUserSettings("cccccccc-2222-3333-4444-555555555555")
	opened, ok := got["openedMenuSections"]
	if !ok {
		t.Fatalf("loadUserSettings returned %#v without openedMenuSections", got)
	}
	// Stored JSON unmarshals to []interface{}, unlike the Go []string default.
	if !reflect.DeepEqual(opened, []interface{}{"/devices"}) {
		t.Fatalf("openedMenuSections = %#v, want preserved [\"/devices\"]", opened)
	}
}

func TestLoadUserSettingsDefaultsForNoRow(t *testing.T) {
	// Set up the pool/table; the no-row path returns defaults without hitting
	// a stored row.
	newUserSettingsDB(t)

	got := loadUserSettings("no-such-user")
	opened, ok := got["openedMenuSections"]
	if !ok {
		t.Fatalf("loadUserSettings returned %#v without openedMenuSections", got)
	}
	if !reflect.DeepEqual(opened, []string{"/entities"}) {
		t.Fatalf("openedMenuSections = %#v, want default [\"/entities\"]", opened)
	}
}
