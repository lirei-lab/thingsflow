--
-- alarm_comment is PARTITION BY RANGE (created_time) but TB classic creates
-- monthly partitions in Java code. We use a single catch-all default
-- partition for simplicity (same approach as ts_kv).
--
CREATE TABLE IF NOT EXISTS alarm_comment_default PARTITION OF alarm_comment DEFAULT;
