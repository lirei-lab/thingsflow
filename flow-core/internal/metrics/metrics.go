// Package metrics exposes a /metrics endpoint in the Prometheus text
// exposition format without pulling in the full prometheus/client_golang
// dependency. The surface is intentionally narrow — counters that
// already exist elsewhere in the codebase get registered here and
// rendered on demand.
//
// Why hand-rolled instead of client_golang: scope. The library brings
// ~12 transitive deps and a `histogram with native buckets` API that
// we don't need yet. When metric variety grows past ~10 counters or
// we need histograms, swap this for the real client — call sites only
// know about Inc()/Add()/Set(), so the migration is local.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

type counter struct {
	help  string
	value *atomic.Uint64
}

type gauge struct {
	help string
	fn   func() float64
}

var (
	mu       sync.RWMutex
	counters = map[string]*counter{}
	gauges   = map[string]*gauge{}
)

// Counter returns the named counter, creating it on first use. Counters
// are monotonic; use Inc/Add only.
func Counter(name, help string) *Cnt {
	mu.Lock()
	defer mu.Unlock()
	c, ok := counters[name]
	if !ok {
		c = &counter{help: help, value: new(atomic.Uint64)}
		counters[name] = c
	}
	return &Cnt{value: c.value}
}

// RegisterGauge wires a sampler function to a gauge name. Called once
// at startup; the sampler runs every scrape.
func RegisterGauge(name, help string, fn func() float64) {
	mu.Lock()
	defer mu.Unlock()
	gauges[name] = &gauge{help: help, fn: fn}
}

// Cnt is the caller-facing counter handle. Methods are inlinable + lock-free.
type Cnt struct{ value *atomic.Uint64 }

func (c *Cnt) Inc()         { c.value.Add(1) }
func (c *Cnt) Add(n uint64) { c.value.Add(n) }
func (c *Cnt) Load() uint64 { return c.value.Load() }

// Handler renders the registered counters + gauges in Prometheus text
// exposition format. Mount at /metrics.
func Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(counters)+len(gauges))
	for n := range counters {
		names = append(names, n)
	}
	for n := range gauges {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		if c, ok := counters[n]; ok {
			fmt.Fprintf(w, "# HELP %s %s\n", n, c.help)
			fmt.Fprintf(w, "# TYPE %s counter\n", n)
			fmt.Fprintf(w, "%s %d\n", n, c.value.Load())
		}
		if g, ok := gauges[n]; ok {
			fmt.Fprintf(w, "# HELP %s %s\n", n, g.help)
			fmt.Fprintf(w, "# TYPE %s gauge\n", n)
			fmt.Fprintf(w, "%s %g\n", n, g.fn())
		}
	}
}
