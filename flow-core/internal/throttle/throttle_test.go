package throttle

import (
	"fmt"
	"net/http/httptest"
	"strings"
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
	attempts.m["fresh-ip"] = &counter{distinct: map[string]struct{}{"a": {}}, attempts: 1, firstFail: now}
	attempts.m["stale-ip-1"] = &counter{distinct: map[string]struct{}{"a": {}}, attempts: 5, firstFail: now.Add(-10 * time.Second)}
	attempts.m["stale-ip-2"] = &counter{distinct: map[string]struct{}{"a": {}}, attempts: 3, firstFail: now.Add(-5 * time.Second)}
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
		RecordFail(ip, Fingerprint("user@example.com", fmt.Sprintf("guess-%d", i)))
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

func reset(t *testing.T) {
	t.Helper()
	attempts.Lock()
	attempts.m = make(map[string]*counter)
	attempts.Unlock()
}

// TestStuckClientDoesNotExhaustBudget reproduces the 2026-08-14 outage.
// An edge gateway retried ONE expired credential once a minute; under
// raw-attempt counting it pinned the shared counter at its cap and
// locked out every other client behind the same ingress for ~10 hours.
// The same credential must consume exactly one slot no matter how often
// it is replayed, leaving room for everyone else.
func TestStuckClientDoesNotExhaustBudget(t *testing.T) {
	reset(t)
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "5")
	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "300")

	const sharedIP = "10.1.23.62" // the ingress everyone arrives behind
	stuck := Fingerprint("tenant@thingsboard.org", "stale-password")

	for i := 0; i < 40; i++ {
		RecordFail(sharedIP, stuck)
	}
	if !Allow(sharedIP) {
		t.Fatal("40 replays of ONE credential exhausted the budget — the outage would recur")
	}

	// Budget still has room for the operator's own typos.
	for i := 0; i < 3; i++ {
		RecordFail(sharedIP, Fingerprint("operator@example.com", fmt.Sprintf("typo-%d", i)))
	}
	if !Allow(sharedIP) {
		t.Error("stuck client plus 3 unrelated typos should still be under the cap of 5")
	}
}

// TestBruteForceStillBlocked is the other half: the relaxation must not
// weaken the actual defence. Varying the password is brute force and
// still has to trip the cap at exactly max-fails.
func TestBruteForceStillBlocked(t *testing.T) {
	reset(t)
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "5")
	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "300")

	const ip = "203.0.113.9"
	for i := 0; i < 5; i++ {
		if !Allow(ip) {
			t.Fatalf("blocked early at attempt %d, before reaching the cap", i)
		}
		RecordFail(ip, Fingerprint("victim@example.com", fmt.Sprintf("candidate-%d", i)))
	}
	if Allow(ip) {
		t.Error("5 DISTINCT passwords must trip the cap — brute force is not throttled")
	}
}

// TestUsernameEnumerationCountsAsDistinct — spraying one password across
// many usernames is enumeration, not a repeat. The fingerprint covers
// the username for exactly this reason.
func TestUsernameEnumerationCountsAsDistinct(t *testing.T) {
	reset(t)
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "5")
	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "300")

	const ip = "203.0.113.11"
	for i := 0; i < 6; i++ {
		RecordFail(ip, Fingerprint(fmt.Sprintf("user%d@example.com", i), "Summer2026!"))
	}
	if Allow(ip) {
		t.Error("one password sprayed across 6 usernames must trip the cap")
	}
}

// TestRawAttemptCapBoundsCPU — ignoring repeats would otherwise let one
// client burn unlimited bcrypt with a single wrong password. The raw cap
// is the backstop, deliberately far above any sane retry rate.
func TestRawAttemptCapBoundsCPU(t *testing.T) {
	reset(t)
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "5")
	t.Setenv("LOGIN_THROTTLE_MAX_ATTEMPTS", "20")
	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "300")

	const ip = "203.0.113.13"
	same := Fingerprint("a@example.com", "one-password")
	for i := 0; i < 20; i++ {
		RecordFail(ip, same)
	}
	if Allow(ip) {
		t.Error("hammering one credential past the raw cap must be denied")
	}
}

// TestSuccessClearsBothCounters — a correct login rehabilitates the key.
func TestSuccessClearsBothCounters(t *testing.T) {
	reset(t)
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "2")
	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "300")

	const ip = "203.0.113.15"
	RecordFail(ip, Fingerprint("u@example.com", "wrong-1"))
	RecordFail(ip, Fingerprint("u@example.com", "wrong-2"))
	if Allow(ip) {
		t.Fatal("setup: expected the key to be blocked")
	}
	RecordSuccess(ip)
	if !Allow(ip) {
		t.Error("a successful login must clear the counters")
	}
}

// TestEmptyFingerprintDegradesToCountingEveryAttempt — a caller that
// cannot compute a fingerprint must fall back to the old strict
// behaviour, never to "no limit".
func TestEmptyFingerprintDegradesToCountingEveryAttempt(t *testing.T) {
	reset(t)
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "3")
	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "300")

	const ip = "203.0.113.17"
	for i := 0; i < 3; i++ {
		RecordFail(ip, "")
	}
	if Allow(ip) {
		t.Error("empty fingerprints must each count, so 3 attempts trip a cap of 3")
	}
}

// TestFingerprintProperties — distinct per credential, stable per call,
// and not a recoverable copy of the password.
func TestFingerprintProperties(t *testing.T) {
	a := Fingerprint("u@example.com", "secret")
	if a != Fingerprint("u@example.com", "secret") {
		t.Error("fingerprint must be stable for the same pair")
	}
	if a == Fingerprint("u@example.com", "secret2") {
		t.Error("different passwords must differ")
	}
	if a == Fingerprint("other@example.com", "secret") {
		t.Error("different usernames must differ")
	}
	if strings.Contains(a, "secret") {
		t.Error("fingerprint leaks the password verbatim")
	}
	// Salted per process: a digest is not portable to another run.
	old := salt
	salt = []byte("a-different-salt")
	defer func() { salt = old }()
	if a == Fingerprint("u@example.com", "secret") {
		t.Error("fingerprint must depend on the per-process salt")
	}
}

// TestDistinctSetStopsGrowingAtCap — once denial is certain there is
// nothing to learn from more credentials, and an attacker must not be
// able to grow the set without bound.
func TestDistinctSetStopsGrowingAtCap(t *testing.T) {
	reset(t)
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "5")
	t.Setenv("LOGIN_THROTTLE_MAX_ATTEMPTS", "100000")
	t.Setenv("LOGIN_THROTTLE_WINDOW_SECONDS", "300")

	const ip = "203.0.113.19"
	for i := 0; i < 1000; i++ {
		RecordFail(ip, Fingerprint("u@example.com", fmt.Sprintf("p-%d", i)))
	}
	attempts.RLock()
	got := len(attempts.m[ip].distinct)
	attempts.RUnlock()
	if got > 5 {
		t.Errorf("distinct set grew to %d entries, want it capped at 5", got)
	}
}
