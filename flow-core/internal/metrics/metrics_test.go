package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCounter_IncAndAdd(t *testing.T) {
	c := Counter("test_counter_total", "test counter")
	c.Inc()
	c.Inc()
	c.Add(40)
	if got := c.Load(); got != 42 {
		t.Errorf("counter load = %d, want 42", got)
	}
}

func TestHandler_RendersPromTextFormat(t *testing.T) {
	Counter("test_handler_hits_total", "renders correctly").Add(7)
	RegisterGauge("test_handler_gauge", "test gauge", func() float64 { return 3.14 })

	rec := httptest.NewRecorder()
	Handler(rec, httptest.NewRequest("GET", "/metrics", nil))

	body := rec.Body.String()
	for _, want := range []string{
		"# HELP test_handler_hits_total renders correctly",
		"# TYPE test_handler_hits_total counter",
		"test_handler_hits_total 7",
		"# HELP test_handler_gauge test gauge",
		"# TYPE test_handler_gauge gauge",
		"test_handler_gauge 3.14",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q\n--- body ---\n%s", want, body)
		}
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", got)
	}
}
