"""Database test harnesses must be confined to a throwaway schema.

Why this exists: 29 harnesses opened FLOW_TEST_PG_DSN and ran
`DROP TABLE IF EXISTS asset CASCADE` (and a dozen siblings) to get a clean
slate. Against the disposable Postgres CI starts as a service container that is
fine. Against any database a human might point that variable at -- a local dev
stack, a shared scratch database, a pilot -- it is a data-loss bug waiting for
one careless export. They also collided with each other; CI hid that by running
`go test -p 1`, so the breakage only reached whoever ran `go test ./...`
locally, where packages run in parallel by default.

`internal/testdb.Scoped` fixes it by handing back a DSN bound to a private
schema, so the existing destructive DDL resolves inside that schema and can
never reach `public`. The rule this test enforces is the one that is easy to
forget: a NEW harness that opens the raw DSN gets none of that protection, and
nothing about it looks wrong in review.

Migrating those harnesses also surfaced real hidden coupling: internal/twin's
tests only passed because internal/customer, internal/dashboard and friends had
created `customer`, `dashboard`, `entity_view`, `device_profile` and
`asset_profile` in the shared `public` schema earlier in the run. The isolation
did not break those tests; it revealed what they had been leaning on.
"""
import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
FLOW_CORE = ROOT / "flow-core"
HELPER = "internal/testdb"

# `sql.Open("postgres", <something>)` where the something is a bare dsn-ish
# variable rather than a testdb.Scoped(...) call.
RAW_OPEN = re.compile(r'sql\.Open\(\s*"postgres"\s*,\s*(?!testdb\.Scoped)([A-Za-z_][\w.]*)\s*\)')


def test_files():
    for p in sorted(FLOW_CORE.rglob("*_test.go")):
        yield p


class DBTestIsolationTest(unittest.TestCase):
    def test_no_harness_opens_an_unconfined_connection(self):
        offenders = []
        for path in test_files():
            text = path.read_text()
            if "FLOW_TEST_PG_DSN" not in text:
                continue
            # The helper's own tests must open an UNCONFINED connection: that is
            # how they place a canary table in `public` and prove the confined
            # side cannot reach it. Exempting them is not a loophole — without
            # the raw connection there is nothing to prove.
            if path.parent.name == "testdb":
                continue
            # A harness that builds its own schema by hand predates the helper
            # and is already confined; it just does it the long way.
            if "CREATE SCHEMA" in text:
                continue
            for m in RAW_OPEN.finditer(text):
                offenders.append(f"{path.relative_to(ROOT)} -> sql.Open(\"postgres\", {m.group(1)})")

        self.assertEqual(
            [],
            offenders,
            "these harnesses open FLOW_TEST_PG_DSN without confinement, so their "
            "DROP TABLE statements reach whatever database the variable points at:\n  "
            + "\n  ".join(offenders)
            + f"\n\nWrap the DSN: sql.Open(\"postgres\", testdb.Scoped(t, dsn)) "
            f"(package flow-core/{HELPER}).",
        )

    def test_the_helper_binds_search_path_as_a_connection_parameter(self):
        """Not as a `SET`.

        sql.DB is a pool. `SET search_path` applies only to the connection that
        served it, so under any concurrency a later query lands on a connection
        still pointed at `public` — the confinement failing open, silently, and
        exactly when load makes it matter. As a DSN parameter the server applies
        it at connection setup, to every connection the pool opens.
        """
        helper = (FLOW_CORE / HELPER / "testdb.go").read_text()
        self.assertIn('"search_path="', helper)
        # Strip comments before asserting absence: the helper's own doc comment
        # explains why `SET search_path` is wrong, so a bare substring check
        # matches the warning rather than the code.
        code = "\n".join(
            l for l in helper.splitlines() if not l.lstrip().startswith("//")
        )
        self.assertNotIn("SET search_path", code)

    def test_catalogue_queries_are_scoped_to_the_current_schema(self):
        """A catalogue query spans the whole database, not the test's schema.

        `SELECT count(*) FROM information_schema.columns WHERE table_name='policy'`
        counts that table in EVERY schema, so once packages run concurrently and
        each holds its own copy the assertion reports "found 2" for a perfectly
        correct migration. `pg_constraint` has the same shape: constraint names
        are unique per schema, not per database, and QueryRow silently takes
        whichever row comes first.

        Measured before the fix: 2 of 5 parallel runs failed. It never showed up
        under CI's `-p 1`, which is exactly why it survived.
        """
        offenders = []
        for path in test_files():
            text = path.read_text()
            for catalogue in ("information_schema.", "pg_constraint", "pg_indexes"):
                if catalogue not in text:
                    continue
                # Every query touching a catalogue must constrain the namespace.
                if "current_schema()" not in text:
                    offenders.append(f"{path.relative_to(ROOT)} ({catalogue})")
        self.assertEqual(
            [],
            offenders,
            "these tests query a database-wide catalogue without pinning it to "
            "current_schema(), so they see other packages' concurrent schemas:\n  "
            + "\n  ".join(sorted(set(offenders))),
        )

    def test_the_helper_drops_what_it_creates(self):
        helper = (FLOW_CORE / HELPER / "testdb.go").read_text()
        self.assertIn("CREATE SCHEMA", helper)
        self.assertIn("DROP SCHEMA IF EXISTS", helper)
        self.assertIn("t.Cleanup", helper)


if __name__ == "__main__":
    unittest.main()
