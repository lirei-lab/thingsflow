package httputil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	WriteJSON(w, http.StatusCreated, map[string]string{"foo": "bar"})

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	var got map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if got["foo"] != "bar" {
		t.Errorf("body[foo] = %q, want bar", got["foo"])
	}
}

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()
	WriteError(w, http.StatusForbidden, "no entry")

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	// TB-classic shape: status, message, errorCode (10), timestamp
	if body["message"] != "no entry" {
		t.Errorf("message = %v, want no entry", body["message"])
	}
	if int(body["errorCode"].(float64)) != 10 {
		t.Errorf("errorCode = %v, want 10", body["errorCode"])
	}
	if body["timestamp"] == nil {
		t.Error("timestamp missing")
	}
}

func TestIntParam(t *testing.T) {
	cases := []struct {
		query    string
		key      string
		def, exp int
	}{
		{"pageSize=42", "pageSize", 10, 42},
		{"pageSize=", "pageSize", 10, 10},
		{"pageSize=abc", "pageSize", 10, 10}, // unparseable falls back
		{"", "pageSize", 7, 7},
		{"page=0", "page", 5, 0},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/?"+c.query, nil)
		got := IntParam(r, c.key, c.def)
		if got != c.exp {
			t.Errorf("IntParam(%q, %q, %d) = %d, want %d", c.query, c.key, c.def, got, c.exp)
		}
	}
}

// Phase 5c — IntParam handed the client's pageSize straight to the SQL LIMIT,
// so ?pageSize=100000000 made Postgres sort and stream a whole tenant table
// into Go maps. PageSize is the single clamp every paginated endpoint now goes
// through: absent/garbage → the handler default, non-positive → the default,
// above the ceiling → the ceiling, anything sane → untouched.
func TestPageSize(t *testing.T) {
	cases := []struct {
		name     string
		query    string
		def, exp int
	}{
		{"absent", "", 10, 10},
		{"unparseable", "pageSize=abc", 10, 10},
		{"empty", "pageSize=", 10, 10},
		{"zero falls back", "pageSize=0", 10, 10},
		{"negative falls back", "pageSize=-1", 10, 10},
		{"normal untouched", "pageSize=42", 10, 42},
		{"at the ceiling", "pageSize=1000", 10, MaxPageSize},
		{"over the ceiling", "pageSize=1001", 10, MaxPageSize},
		{"DoS value clamped", "pageSize=100000000", 10, MaxPageSize},
		{"overflow-ish value clamped", "pageSize=2147483647", 100, MaxPageSize},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/?"+c.query, nil)
		if got := PageSize(r, c.def); got != c.exp {
			t.Errorf("%s: PageSize(%q, %d) = %d, want %d", c.name, c.query, c.def, got, c.exp)
		}
	}
}

// ClampPageSize is the body-driven twin (entity-query pageLinks) — same bound,
// and it must never return 0 even when the caller passes a bogus default.
func TestClampPageSize(t *testing.T) {
	cases := []struct{ n, def, exp int }{
		{50, 100, 50},
		{0, 100, 100},
		{-5, 100, 100},
		{MaxPageSize + 1, 100, MaxPageSize},
		{0, 0, 1},
	}
	for _, c := range cases {
		if got := ClampPageSize(c.n, c.def); got != c.exp {
			t.Errorf("ClampPageSize(%d, %d) = %d, want %d", c.n, c.def, got, c.exp)
		}
	}
}

func TestExtractEntityID(t *testing.T) {
	cases := []struct {
		name string
		body map[string]interface{}
		key  string
		want string
	}{
		{"missing", map[string]interface{}{}, "id", ""},
		{"nil value", map[string]interface{}{"id": nil}, "id", ""},
		{"bare string", map[string]interface{}{"id": "abc"}, "id", "abc"},
		{"TB shape", map[string]interface{}{"id": map[string]interface{}{"id": "xyz", "entityType": "DEVICE"}}, "id", "xyz"},
		{"unknown shape", map[string]interface{}{"id": 42}, "id", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ExtractEntityID(c.body, c.key)
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestCoalesceStr(t *testing.T) {
	val := "hello"
	empty := ""
	if got := CoalesceStr(nil, "fb"); got != "fb" {
		t.Errorf("nil → %q, want fb", got)
	}
	if got := CoalesceStr(&empty, "fb"); got != "fb" {
		t.Errorf("empty → %q, want fb (matches original behavior)", got)
	}
	if got := CoalesceStr(&val, "fb"); got != "hello" {
		t.Errorf("hello → %q", got)
	}
}

func TestSetOptional(t *testing.T) {
	val := "x"
	m := map[string]interface{}{}
	SetOptional(m, "k1", nil)
	SetOptional(m, "k2", &val)
	if m["k1"] != nil {
		t.Errorf("nil pointer should leave key as nil, got %v", m["k1"])
	}
	if m["k2"] != "x" {
		t.Errorf("k2 = %v, want x", m["k2"])
	}
}

func TestLooksLikeUUID(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"13814000-1dd2-11b2-8080-808080808080", true},
		{"abcdef00-0000-0000-0000-000000000000", true},
		{"too-short", false},
		{"", false},
		{strings.Repeat("a", 36), false}, // 36 chars but no dashes
	}
	for _, c := range cases {
		if got := LooksLikeUUID(c.in); got != c.want {
			t.Errorf("LooksLikeUUID(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
