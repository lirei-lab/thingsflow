// Package desiredstate delivers twin desired state to devices and converges
// device-reported state back into the twin (R5). Delivery is MQTT retained on
// a per-device topic (thingsflow/devices/<mqttId>/desired) so a device receives
// the current desired state on connect/reconnect; reported convergence reads
// device-reported attributes and merges them into the twin record + KV. Both
// paths are control-plane (fire-and-forget) and never touch the ingest hot path
// (Bento / the rmqtt NATS egress bridge are untouched).
package desiredstate
