import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


class PostgresLatestStateCleanupTest(unittest.TestCase):
    def test_bootstrap_schema_no_longer_creates_device_latest_state(self):
        sql_dir = ROOT / "k8s" / "helm" / "thingsflow" / "files" / "sql"

        self.assertFalse((sql_dir / "11_schema-device-latest-state.sql").exists())
        for path in sql_dir.glob("*.sql"):
            self.assertNotIn("device_latest_state", path.read_text())

    def test_migration_drops_obsolete_device_latest_state_snapshot(self):
        migration = (ROOT / "flow-core/internal/migrations/migrations/0013_drop_device_latest_state.up.sql").read_text()
        rollback = (ROOT / "flow-core/internal/migrations/migrations/0013_drop_device_latest_state.down.sql").read_text()

        self.assertIn("DROP TABLE IF EXISTS device_latest_state", migration)
        self.assertIn("CREATE TABLE IF NOT EXISTS device_latest_state", rollback)


if __name__ == "__main__":
    unittest.main()
