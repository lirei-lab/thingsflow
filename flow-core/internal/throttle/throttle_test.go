package throttle

import (
	"net/http/httptest"
	"testing"
	"time"
)

// TestClientIPIgnoresClientSuppliedXFF — Phase 5c. ClientIP used to take the
// LEFTMOST X-Forwarded-For element, which is exactly the one the client
// supplies (proxies append on the right). Enabling the documented
// LOGIN_THROTTLE_TRUST_PROXY therefore disabled the throttle: rotate the
// header, get a fresh budget, burn unlimited bcrypt CPU. The key must now come
// from the hop our own proxy appended, whatever the client puts in front of it.
func TestClientIPIgnoresClientSuppliedXFF(t *testing.T) {
	t.Setenv("LOGIN_THROTTLE_TRUST_PROXY", "true")

	const realPeer = "203.0.113.7" // what our ingress observed

	// One trusted hop (the default): the rightmost element is ours.
	spoofs := []string{
		"9.9.9.9, " + realPeer,
		"1.1.1.1, 2.2.2.2, " + realPeer,
		"evil, evil, evil, " + realPeer,
	}
	for _, xff := range spoofs {
		r := httptest.NewRequest("POST", "/api/auth/login", nil)
		r.RemoteAddr = "10.0.0.1:5555"
		r.Header.Set("X-Forwarded-For", xff)
		if got := ClientIP(r); got != realPeer {
			t.Errorf("XFF %q → ClientIP %q, want %q (proxy-appended hop)", xff, got, realPeer)
		}
	}

	// Rotating the spoofed prefix must NOT change the throttle key — that is
	// the whole attack.
	r1 := httptest.NewRequest("POST", "/api/auth/login", nil)
	r1.RemoteAddr = "10.0.0.1:5555"
	r1.Header.Set("X-Forwarded-For", "attacker-run-1, "+realPeer)
	r2 := httptest.NewRequest("POST", "/api/auth/login", nil)
	r2.RemoteAddr = "10.0.0.1:5555"
	r2.Header.Set("X-Forwarded-For", "attacker-run-2, "+realPeer)
	if ClientIP(r1) != ClientIP(r2) {
		t.Errorf("rotating the client-supplied XFF prefix changed the throttle key: %q vs %q", ClientIP(r1), ClientIP(r2))
	}

	// Two trusted hops: take the second value from the right.
	t.Setenv("LOGIN_THROTTLE_TRUSTED_HOPS", "2")
	r3 := httptest.NewRequest("POST", "/api/auth/login", nil)
	r3.RemoteAddr = "10.0.0.1:5555"
	r3.Header.Set("X-Forwarded-For", "spoofed, "+realPeer+", 172.16.0.9")
	if got := ClientIP(r3); got != realPeer {
		t.Errorf("2 hops → ClientIP %q, want %q", got, realPeer)
	}

	// Header shorter than the trusted hop count is client-truncated: fall back
	// to RemoteAddr instead of trusting it.
	r4 := httptest.NewRequest("POST", "/api/auth/login", nil)
	r4.RemoteAddr = "10.0.0.1:5555"
	r4.Header.Set("X-Forwarded-For", "spoofed-only")
	if got := ClientIP(r4); got != "10.0.0.1" {
		t.Errorf("truncated XFF → ClientIP %q, want the RemoteAddr host", got)
	}
}

// TestClientIPWithoutTrustProxy — with the flag off, headers are ignored
// entirely and RemoteAddr wins (the default deployment shape).
func TestClientIPWithoutTrustProxy(t *testing.T) {
	r := httptest.NewRequest("POST", "/api/auth/login", nil)
	r.RemoteAddr = "198.51.100.4:44321"
	r.Header.Set("X-Forwarded-For", "1.1.1.1")
	r.Header.Set("X-Real-IP", "2.2.2.2")
	if got := ClientIP(r); got != "198.51.100.4" {
		t.Errorf("ClientIP = %q, want the RemoteAddr host", got)
	}
}

// TestJanitorPrunesExpired — regression guard for H-4 in
// docs/SECURITY_AUDIT.md. The throttle map used to grow unbounded;
// an attacker rotating distinct IPs would leak memory until the pod
// OOM'd. The janitor (or pruneExpiredForTest in this test) walks the
// map every minute and drops counters whose firstFail is older than
// 2× the configured window.
func TestJanitorPrunesExpired(t *testing.T) {
	// Reset the package-level map so other tests don't pollute us.
	attempts.Lock()
	attempts.m = make(map[string]*counter)
	attempts.Unlock()

	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "1") // 1s window → cutoff at 2s

	// Three IPs, two of them stale (synthetic firstFail in the past).
	now := time.Now()
	attempts.Lock()
	attempts.m["fresh-ip"] = &counter{fails: 1, firstFail: now}
	attempts.m["stale-ip-1"] = &counter{fails: 5, firstFail: now.Add(-10 * time.Second)}
	attempts.m["stale-ip-2"] = &counter{fails: 3, firstFail: now.Add(-5 * time.Second)}
	attempts.Unlock()

	if got := SizeForTest(); got != 3 {
		t.Fatalf("setup: size = %d, want 3", got)
	}

	pruneExpiredForTest()

	if got := SizeForTest(); got != 1 {
		t.Errorf("after prune: size = %d, want 1 (only fresh-ip should survive)", got)
	}
	attempts.RLock()
	_, freshKept := attempts.m["fresh-ip"]
	_, stale1Kept := attempts.m["stale-ip-1"]
	attempts.RUnlock()
	if !freshKept {
		t.Error("fresh-ip got pruned (should have stayed)")
	}
	if stale1Kept {
		t.Error("stale-ip-1 survived pruning")
	}
}

// TestAllowAfterWindow — adjacent behaviour: a counter that exceeded
// max-fails should NOT block once its window expired (Allow returns
// true), even before the janitor reclaims the slot.
func TestAllowAfterWindow(t *testing.T) {
	attempts.Lock()
	attempts.m = make(map[string]*counter)
	attempts.Unlock()
	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "1")
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "3")

	const ip = "1.2.3.4"
	for i := 0; i < 5; i++ {
		RecordFail(ip)
	}
	if Allow(ip) {
		t.Error("expected Allow=false right after 5 fails > max=3")
	}
	// Synthetically age the counter past the window
	attempts.Lock()
	attempts.m[ip].firstFail = time.Now().Add(-2 * time.Second)
	attempts.Unlock()
	if !Allow(ip) {
		t.Error("expected Allow=true after window expired")
	}
}
