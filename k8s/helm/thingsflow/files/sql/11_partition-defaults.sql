--
-- Default partitions for TB-compatible event/audit tables.
--
-- ThingsBoard classic creates time partitions from Java services. ThingsFlow keeps
-- the hot telemetry path outside flow-core, so the compatibility database needs
-- safe catch-all partitions for tables that still receive control-plane events.
--

CREATE TABLE IF NOT EXISTS audit_log_default PARTITION OF audit_log DEFAULT;
CREATE TABLE IF NOT EXISTS rule_node_debug_event_default PARTITION OF rule_node_debug_event DEFAULT;
CREATE TABLE IF NOT EXISTS rule_chain_debug_event_default PARTITION OF rule_chain_debug_event DEFAULT;
CREATE TABLE IF NOT EXISTS stats_event_default PARTITION OF stats_event DEFAULT;
CREATE TABLE IF NOT EXISTS lc_event_default PARTITION OF lc_event DEFAULT;
CREATE TABLE IF NOT EXISTS error_event_default PARTITION OF error_event DEFAULT;
