// Package throttle is an in-memory per-IP login attempt limiter.
// Caps brute-force attempts at LOGIN_THROTTLE_MAX_FAILS over
// LOGIN_THROTTLE_WINDOW_SECONDS. Default: 5 fails / 5 minutes;
// successful login resets the counter.
//
// Per-process state, no Redis/external store. Each bridge replica
// tracks its own counters — fine in practice because device-edge HTTP
// backends + TB UI sessions stick to a single replica, and the worst
// case (attacker sprays N replicas) still divides the success rate
// by N. For multi-replica hardening, swap the map for a Redis-backed
// counter in this package — callers don't change.
//
// Behind an L7 load balancer set LOGIN_THROTTLE_TRUST_PROXY=true so
// ClientIP reads from X-Forwarded-For; otherwise it uses RemoteAddr.
package throttle

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type counter struct {
	fails     int
	firstFail time.Time
}

var attempts = struct {
	sync.RWMutex
	m map[string]*counter
}{m: make(map[string]*counter)}

// init starts the janitor that prunes expired counters. Without this
// the map grew unbounded — an attacker rotating through 100k distinct
// IPs creates 100k entries × ~32 bytes ≈ 3 MB. Over weeks → OOM kill.
// The janitor walks the map every minute and deletes counters whose
// firstFail is older than 2× the configured window. The 2× margin
// keeps a fresh counter for an IP that comes back right after expiry
// instead of a stale one.
func init() {
	go janitor()
}

func janitor() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for range t.C {
		_, window := config()
		cutoff := time.Now().Add(-2 * window)
		attempts.Lock()
		for ip, c := range attempts.m {
			if c.firstFail.Before(cutoff) {
				delete(attempts.m, ip)
			}
		}
		attempts.Unlock()
	}
}

func config() (maxFails int, window time.Duration) {
	if v, err := strconv.Atoi(envOr("LOGIN_THROTTLE_MAX_FAILS", "5")); err == nil && v > 0 {
		maxFails = v
	} else {
		maxFails = 5
	}
	if v, err := strconv.Atoi(envOr("LOGIN_THROTTLE_WINDOW_SECONDS", "300")); err == nil && v > 0 {
		window = time.Duration(v) * time.Second
	} else {
		window = 5 * time.Minute
	}
	return
}

// ClientIP extracts the client IP, honoring X-Forwarded-For when
// LOGIN_THROTTLE_TRUST_PROXY is set.
func ClientIP(r *http.Request) string {
	if envOr("LOGIN_THROTTLE_TRUST_PROXY", "") != "" {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if i := strings.IndexByte(xff, ','); i >= 0 {
				return strings.TrimSpace(xff[:i])
			}
			return strings.TrimSpace(xff)
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return strings.TrimSpace(xri)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Allow returns true if the IP is under the failure cap.
func Allow(ip string) bool {
	if ip == "" {
		return true
	}
	maxFails, window := config()
	attempts.RLock()
	c, ok := attempts.m[ip]
	attempts.RUnlock()
	if !ok {
		return true
	}
	if time.Since(c.firstFail) > window {
		return true
	}
	return c.fails < maxFails
}

// RecordFail bumps the failure counter for an IP.
func RecordFail(ip string) {
	if ip == "" {
		return
	}
	_, window := config()
	attempts.Lock()
	defer attempts.Unlock()
	c, ok := attempts.m[ip]
	if !ok || time.Since(c.firstFail) > window {
		attempts.m[ip] = &counter{fails: 1, firstFail: time.Now()}
		return
	}
	c.fails++
}

// RecordSuccess clears the failure counter for an IP.
func RecordSuccess(ip string) {
	if ip == "" {
		return
	}
	attempts.Lock()
	delete(attempts.m, ip)
	attempts.Unlock()
}

// pruneExpiredForTest exposes the eviction path to tests without
// waiting on the janitor's 60s ticker. Walks the map once with the
// same cutoff logic.
func pruneExpiredForTest() {
	_, window := config()
	cutoff := time.Now().Add(-2 * window)
	attempts.Lock()
	for ip, c := range attempts.m {
		if c.firstFail.Before(cutoff) {
			delete(attempts.m, ip)
		}
	}
	attempts.Unlock()
}

// SizeForTest reports the number of tracked IPs. Tests use this to
// assert the janitor's eviction worked.
func SizeForTest() int {
	attempts.RLock()
	defer attempts.RUnlock()
	return len(attempts.m)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
