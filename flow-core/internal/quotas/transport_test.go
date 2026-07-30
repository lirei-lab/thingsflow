package quotas

import (
	"sync"
	"testing"
)

func cleanup(t *testing.T, tenantID string) {
	t.Helper()
	t.Cleanup(func() {
		InvalidateCache(tenantID)
		ResetForTest(tenantID)
	})
}

func TestAllowTransport_RespectsCap(t *testing.T) {
	tenant := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	SeedForTest(tenant, Limits{MaxTransportMessages: 5})
	cleanup(t, tenant)

	for i := 1; i <= 5; i++ {
		if !AllowTransport(tenant) {
			t.Fatalf("call %d should be allowed (cap=5)", i)
		}
	}
	for i := 6; i <= 10; i++ {
		if AllowTransport(tenant) {
			t.Errorf("call %d should be rejected (cap=5)", i)
		}
	}
	if got := DropCount(); got < 5 {
		t.Errorf("drop count = %d, want ≥ 5", got)
	}
}

func TestAllowTransport_NoLimitMeansUnlimited(t *testing.T) {
	tenant := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	SeedForTest(tenant, Limits{MaxTransportMessages: 0})
	cleanup(t, tenant)

	for i := 0; i < 1000; i++ {
		if !AllowTransport(tenant) {
			t.Fatalf("0 limit should allow everything; rejected at i=%d", i)
		}
	}
}

func TestAllowTransport_ConcurrentSafe(t *testing.T) {
	tenant := "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	SeedForTest(tenant, Limits{MaxTransportMessages: 100})
	cleanup(t, tenant)

	var allowed, rejected int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if AllowTransport(tenant) {
					mu.Lock()
					allowed++
					mu.Unlock()
				} else {
					mu.Lock()
					rejected++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	// 10 workers × 50 calls = 500 attempts. Cap=100 → exactly 100 allowed
	// and 400 rejected (assuming the test runs within one minute window).
	if allowed != 100 {
		t.Errorf("concurrent allowed = %d, want 100", allowed)
	}
	if rejected != 400 {
		t.Errorf("concurrent rejected = %d, want 400", rejected)
	}
}
