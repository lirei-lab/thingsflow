package natsutil

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// apply resolves the option list into the struct nats.Connect would build.
func apply(t *testing.T, opts []nats.Option) nats.Options {
	t.Helper()
	o := nats.GetDefaultOptions()
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			t.Fatalf("applying option: %v", err)
		}
	}
	return o
}

// The regression this whole package exists for: nats.go's default of 60
// attempts makes the client give up and close permanently after ~2 minutes of
// server downtime, leaving a Running process with a dead subscription.
func TestOptionsReconnectsForever(t *testing.T) {
	if def := nats.GetDefaultOptions(); def.MaxReconnect >= 0 {
		t.Logf("guarding against nats.go default MaxReconnect=%d", def.MaxReconnect)
	}
	o := apply(t, Options("test-client"))
	if o.MaxReconnect != -1 {
		t.Fatalf("MaxReconnect = %d, want -1 (unlimited); a bounded count re-opens the 2026-08-28 silent-halt hole", o.MaxReconnect)
	}
	if !o.AllowReconnect {
		t.Fatal("AllowReconnect = false, want true")
	}
	if o.ReconnectWait != 2*time.Second {
		t.Fatalf("ReconnectWait = %v, want 2s", o.ReconnectWait)
	}
	if o.ReconnectJitter <= 0 {
		t.Fatalf("ReconnectJitter = %v, want > 0 so simultaneous consumers do not retry in lockstep", o.ReconnectJitter)
	}
	if o.Timeout != 5*time.Second {
		t.Fatalf("Timeout = %v, want 5s", o.Timeout)
	}
	if o.Name != "test-client" {
		t.Fatalf("Name = %q, want %q", o.Name, "test-client")
	}
}

// The 2026-08-28 outage was invisible in the logs of every process that had
// already subscribed: nothing reported the disconnect.
func TestOptionsInstallsTransitionHandlers(t *testing.T) {
	o := apply(t, Options("test-client"))
	if o.DisconnectedErrCB == nil {
		t.Error("DisconnectErrHandler not installed: a silent disconnect is how the halt went unnoticed")
	}
	if o.ReconnectedCB == nil {
		t.Error("ReconnectHandler not installed")
	}
	if o.ClosedCB == nil {
		t.Error("ClosedHandler not installed")
	}
}

// Call sites must be able to add their own behaviour (the materializer turns a
// close into an exit) without losing the shared policy.
func TestOptionsExtrasWinAndPolicySurvives(t *testing.T) {
	called := false
	o := apply(t, Options("test-client", nats.ClosedHandler(func(*nats.Conn) { called = true })))
	if o.MaxReconnect != -1 {
		t.Fatalf("extras clobbered the reconnect policy: MaxReconnect = %d", o.MaxReconnect)
	}
	if o.ClosedCB == nil {
		t.Fatal("ClosedCB nil")
	}
	o.ClosedCB(nil)
	if !called {
		t.Fatal("call-site ClosedHandler did not override the package default")
	}
}
