package throttle

import (
	"testing"
	"time"
)

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
