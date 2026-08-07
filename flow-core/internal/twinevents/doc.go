// Package twinevents publishes durable control-plane twin events (R4). Every
// twin/attribute/relation change in the control plane emits a best-effort
// event onto TF_TWIN_EVENTS (subjects tf.twin.events.<tenant>.<type>.<id>);
// the stores remain the source of truth — the journal feeds fan-out and
// auditing, never correctness. Mirrors the usage publisher posture: fire and
// forget, graceful disable when NATS is unreachable, never blocks a write.
package twinevents
