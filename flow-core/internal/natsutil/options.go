// Package natsutil owns the ONE NATS client connection policy shared by every
// flow-core process. It exists because nats.go's defaults silently lose the
// data plane.
//
// nats.go defaults to MaxReconnects(60) with ReconnectWait(2s): after roughly
// two minutes of an unreachable server the client stops trying and CLOSES the
// connection for good. Every later operation then returns "nats: connection
// closed" -- forever, even once the server is back. Nothing in the client
// re-opens it.
//
// That is not theoretical. On 2026-08-28 the production NATS pod restarted and
// its RWO volume took longer than the reconnect budget to re-attach. All five
// data-plane consumers (three Bento, the entity writer, the alarm materializer)
// burned through their 60 attempts, closed, and sat there Running-and-Ready
// with dead subscriptions. Telemetry ingest was halted for days; only the
// nats-consumer-guard CronJob noticed, and only after the fact.
//
// The policy here removes the give-up: MaxReconnects(-1) means the client
// reconnects for as long as the process lives, and a NATS outage of any length
// resolves itself when NATS returns. The handlers make each transition visible,
// because the previous failure was invisible in the logs of every process that
// had already subscribed.
//
// Every nats.Connect in this repository must go through Options. A bare
// nats.Connect(url) is the bug, not a shortcut.
package natsutil

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	// connectTimeout bounds the INITIAL dial only; it is not a reconnect budget.
	connectTimeout = 5 * time.Second
	// reconnectWait matches nats.go's own default. The fix is the unlimited
	// attempt count, not a faster retry -- a tighter wait would only hammer a
	// server that is already struggling.
	reconnectWait = 2 * time.Second
	// reconnectJitter spreads the retries of the several consumers that all
	// reconnect at the same instant after a server restart.
	reconnectJitter = 500 * time.Millisecond
)

// Options returns the connection options every nats.Connect call must use,
// followed by any call-site extras (which may override, since nats.Option
// application is last-write-wins).
//
// name is the client name reported by the NATS server's /connz endpoint; make
// it identify the process, so an operator reading connz can tell which of them
// is missing.
func Options(name string, extra ...nats.Option) []nats.Option {
	opts := []nats.Option{
		nats.Name(name),
		nats.Timeout(connectTimeout),
		// The whole point of this package. -1 == never stop reconnecting.
		nats.MaxReconnects(-1),
		nats.ReconnectWait(reconnectWait),
		nats.ReconnectJitter(reconnectJitter, reconnectJitter),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			// WARN, not ERROR: with unlimited reconnects a disconnect is a
			// transient the client is expected to ride out on its own.
			slog.Warn("nats_disconnected", slog.String("client", name), slog.Any("err", err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("nats_reconnected", slog.String("client", name), slog.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			// With MaxReconnects(-1) this fires only on a deliberate Close()
			// (shutdown) -- so it is informational here. Processes whose only
			// job is a subscription pass their own ClosedHandler to turn it
			// into an exit; see cmd/alarm-materializer.
			slog.Info("nats_connection_closed", slog.String("client", name))
		}),
	}
	return append(opts, extra...)
}

// Connect dials url with the shared policy. It is sugar over
// nats.Connect(url, Options(name, extra...)...) so call sites cannot forget the
// spread.
func Connect(url, name string, extra ...nats.Option) (*nats.Conn, error) {
	if url == "" {
		url = nats.DefaultURL
	}
	return nats.Connect(url, Options(name, extra...)...)
}
