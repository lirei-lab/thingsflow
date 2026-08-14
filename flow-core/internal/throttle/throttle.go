// Package throttle is an in-memory per-IP login attempt limiter.
//
// It counts DISTINCT failed credentials, not raw attempts. Brute force
// means trying many different passwords; a client stuck retrying one
// stale credential is a broken client, and the two deserve different
// answers. Counting raw attempts cannot tell them apart, and that is
// not a hypothetical: on 2026-08-14 an edge gateway retrying a single
// expired credential once a minute held the counter at its cap and
// blocked login for every client behind the same ingress for ~10 hours.
// Under the distinct-credential rule that gateway consumes one slot
// forever instead of the whole budget, while an attacker varying the
// password still trips the cap at LOGIN_THROTTLE_MAX_FAILS.
//
// Two caps run per key, both over LOGIN_THROTTLE_WINDOW_SECONDS:
//
//   - LOGIN_THROTTLE_MAX_FAILS (default 5) — distinct credentials.
//     This is the brute-force gate.
//   - LOGIN_THROTTLE_MAX_ATTEMPTS (default 60) — raw attempts. Ignoring
//     repeats would otherwise let one client burn unlimited bcrypt CPU
//     (~100ms each) by hammering a single wrong password, so repeats are
//     free only up to this much higher ceiling. A stuck client retrying
//     on a sane interval never reaches it; a hammer does.
//
// Successful login resets both counters.
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
// X-Forwarded-For is read RIGHT to left, never left to right: proxies
// APPEND the peer they saw, so the leftmost element is whatever the
// client typed and the rightmost is the only one our own proxy wrote.
// Set LOGIN_THROTTLE_TRUSTED_HOPS=N when N proxies sit in front of
// flow-core (default 1); ClientIP then takes the Nth value from the
// right, which is the address the outermost trusted proxy observed.
package throttle

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type counter struct {
	// distinct holds one entry per failed credential seen in this
	// window. Its SIZE is the brute-force signal. Growth is bounded by
	// maxFails: once the cap is reached Allow already denies, so there
	// is nothing to learn from recording further credentials and we
	// stop — otherwise an attacker could grow this set without limit.
	distinct  map[string]struct{}
	attempts  int
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

// maxAttempts caps raw attempts per window, repeats included. It exists
// only to bound bcrypt CPU when the same wrong credential is replayed;
// the brute-force decision belongs to maxFails. Keep it well above any
// legitimate retry rate — a client retrying once a minute produces 5 per
// default window, an order of magnitude below this.
func maxAttempts() int {
	if v, err := strconv.Atoi(envOr("LOGIN_THROTTLE_MAX_ATTEMPTS", "60")); err == nil && v > 0 {
		return v
	}
	return 60
}

// Fingerprint identifies a credential PAIR without retaining it. The
// password never leaves this function: what is stored is a truncated
// digest under a per-process random salt, so the value is useless in
// another process and cannot be matched against precomputed tables.
// Username is included because "same password, many usernames" is
// enumeration and must count as distinct attempts, not as one repeat.
func Fingerprint(username, password string) string {
	h := sha256.New()
	h.Write(salt)
	h.Write([]byte(username))
	h.Write([]byte{0})
	h.Write([]byte(password))
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// salt is regenerated per process, so restarts invalidate old digests
// and nothing derived from a password outlives the process that saw it.
var salt = func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// A salt we cannot randomize would make digests comparable
		// across processes. Fall back to something process-unique
		// rather than to a constant.
		return []byte(strconv.FormatInt(time.Now().UnixNano(), 36) + strconv.Itoa(os.Getpid()))
	}
	return b
}()

// trustedHops reports how many proxies sit between the client and this
// process. Defaults to 1 — a single ingress/L7 LB, the shape we deploy.
func trustedHops() int {
	if v, err := strconv.Atoi(envOr("LOGIN_THROTTLE_TRUSTED_HOPS", "1")); err == nil && v > 0 {
		return v
	}
	return 1
}

// ClientIP extracts the client IP, honoring X-Forwarded-For when
// LOGIN_THROTTLE_TRUST_PROXY is set.
//
// The hop is picked from the RIGHT. Every element left of the ones our
// own infrastructure appended is client-controlled: reading the leftmost
// value — as this did before Phase 5c — meant an attacker could send a
// fresh X-Forwarded-For per request, get a fresh counter every time and
// burn unlimited bcrypt CPU, i.e. enabling the documented
// LOGIN_THROTTLE_TRUST_PROXY setting disabled the throttle. With N
// trusted hops the value at index len-N is the address the outermost
// trusted proxy actually observed. If the header is shorter than N hops
// the client truncated/omitted it, so we fall back to the leftmost
// remaining value only after exhausting the trusted ones — and if the
// header carries a single element with N>1 we prefer RemoteAddr, which
// is unforgeable.
func ClientIP(r *http.Request) string {
	if envOr("LOGIN_THROTTLE_TRUST_PROXY", "") != "" {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			idx := len(parts) - trustedHops()
			if idx >= 0 && idx < len(parts) {
				if ip := strings.TrimSpace(parts[idx]); ip != "" {
					return ip
				}
			}
			// Fewer elements than trusted hops: the header cannot be the
			// one our proxies wrote. Fall through to RemoteAddr rather
			// than trust a client-supplied value.
		} else if xri := r.Header.Get("X-Real-IP"); xri != "" {
			// Single-valued and overwritten (not appended) by the proxy,
			// so it is only consulted when no XFF is present at all.
			return strings.TrimSpace(xri)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Allow reports whether another verification may run for this key. It
// denies once EITHER cap is reached: too many distinct credentials
// (brute force) or too many raw attempts (CPU hammering).
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
	return len(c.distinct) < maxFails && c.attempts < maxAttempts()
}

// RecordFail registers one failed login. fingerprint identifies the
// credential pair (see Fingerprint); a repeat of one already seen in
// this window raises only the raw-attempt count, never the distinct
// count that gates brute force. Passing an empty fingerprint is treated
// as a distinct failure, so a caller that cannot compute one degrades to
// the old count-every-attempt behaviour rather than to no limit at all.
func RecordFail(ip, fingerprint string) {
	if ip == "" {
		return
	}
	maxFails, window := config()
	attempts.Lock()
	defer attempts.Unlock()
	c, ok := attempts.m[ip]
	if !ok || time.Since(c.firstFail) > window {
		c = &counter{distinct: make(map[string]struct{}), firstFail: time.Now()}
		attempts.m[ip] = c
	}
	c.attempts++
	if fingerprint == "" {
		// Unknown credential: cannot dedupe, so count it on its own.
		c.distinct[strconv.Itoa(c.attempts)] = struct{}{}
		return
	}
	if _, seen := c.distinct[fingerprint]; seen {
		return
	}
	// Stop growing the set once denial is certain — see counter.distinct.
	if len(c.distinct) >= maxFails {
		return
	}
	c.distinct[fingerprint] = struct{}{}
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
