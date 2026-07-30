--
-- Default partition for ts_kv. Without this, every INSERT fails with
--   "no partition of relation 'ts_kv' found for row"
-- TB classic creates monthly partitions on demand from Java; we keep
-- things simple and let everything fall into a single catch-all partition.
-- For multi-year deployments switch to monthly partitions + a partman
-- background worker.
--
CREATE TABLE IF NOT EXISTS ts_kv_default PARTITION OF ts_kv DEFAULT;
