"""The consumer guard must judge progress, not queue depth.

Why this exists: a rolling node maintenance restarted NATS and left three Bento
consumers (latest-kv, alarms, entity-greptimedb) holding dead subscriptions.
All three reported Running 1/1 for hours -- neither /ping nor /ready detects a
subscription that is dead underneath a live connection -- and the halt was found
only when a human ran `nats consumer info` by hand.

The obvious detector is a backlog threshold, and it is wrong in the direction
that makes a guard worthless. latest-kv runs max_ack_pending=1 as a deliberate
single-writer serialization mechanism, so under load it holds a large and often
GROWING backlog while working perfectly: measured against a real NATS 2.10.26
server at a sustained backlog of 6,729 while its ack floor advanced 61,179 ->
100,526. A depth rule alerts there, every tick, forever.

So the guard checks push_bound (is anything subscribed to the deliver subject)
and whether ack_floor.consumer_seq MOVES between two samples. These tests pin
the properties that are easy to "simplify" away later and that no rendering
error would surface on its own.

Verified in a NATS 2.10.26 sandbox across all four branches before this test was
written: unbound with 93,000 waiting exits 1; bound but never acking (4,000
waiting, ack floor frozen) exits 1; an expected-but-absent durable exits 1; and a
serialized consumer with 10,974 waiting and an advancing ack floor exits 0.
"""
import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
CHART = ROOT / "k8s/helm/thingsflow"
SCRIPTS = "templates/nats-consumer-guard-scripts.yaml"
CRONJOB = "templates/nats-consumer-guard.yaml"


def render(template, *set_args, values=None):
    cmd = ["helm", "template", "tf", str(CHART), "--show-only", template]
    if values:
        cmd += ["-f", str(CHART / values)]
    for arg in set_args:
        cmd += ["--set", arg]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if out.returncode != 0:
        raise AssertionError(f"helm template failed: {out.stderr}")
    return out.stdout


class ConsumerGuardScriptTest(unittest.TestCase):
    def setUp(self):
        self.script = render(SCRIPTS)

    def test_progress_is_measured_on_the_consumer_ack_floor(self):
        # consumer_seq, NOT stream_seq: TF_RAW is discard-old with a max_bytes
        # cap, so retention deleting messages out from under a STALLED consumer
        # drags its stream-side floor forward and reads as false progress.
        self.assertIn("ack_floor.consumer_seq", self.script)
        self.assertNotIn("ack_floor.stream_seq", self.script)

    def test_backlog_alone_never_decides(self):
        # Backlog may only be used to short-circuit to HEALTHY (nothing waiting
        # means nothing is stuck). It must never be compared against a threshold
        # or against its own earlier value -- that is the rule that would report
        # a serialized latest-kv under load as dead.
        self.assertIn('[ "$backlog" = "0" ]', self.script)
        self.assertNotIn("$backlog -gt", self.script)
        self.assertNotIn("$backlog1", self.script)
        self.assertNotIn("FLOOR", self.script)

    def test_absent_push_bound_is_read_as_false(self):
        # push_bound is omitempty: the server OMITS it when no subscriber is
        # bound, so it arrives as null. Without the `// false` default a naive
        # test would treat the stalled case as healthy -- the exact blind spot.
        self.assertIn(".push_bound // false", self.script)

    def test_an_alert_requires_the_fault_in_both_samples(self):
        # A helm upgrade rolls the Bento pods and leaves a brief window with no
        # subscriber. Firing on that would alert on every deployment.
        self.assertIn('[ "$bound" != "true" ] && [ "$bound1" != "true" ]', self.script)

    def test_lists_keep_their_last_element(self):
        # printf '%s' without a trailing newline makes `while read` silently drop
        # the LAST element of every comma-separated list -- and the last element
        # of the rendered expected set is the alarm-materializer durable, the one
        # consumer with no Kubernetes probes at all.
        self.assertIn("printf '%s\\n' \"$1\" | tr ',' '\\n'", self.script)

    def test_unreadable_consumers_alert_rather_than_pass(self):
        # Not being able to read a consumer is not evidence of health.
        self.assertIn("a halt cannot be ruled out", self.script)

    def test_unreachable_nats_is_one_alert_not_a_storm(self):
        self.assertIn("NATS unreachable", self.script)
        self.assertIn("thingsflow_nats_consumer_progress_ok 0", self.script)


class ConsumerGuardCronJobTest(unittest.TestCase):
    def test_expected_set_tracks_the_history_store(self):
        # The expected set must mirror the same gates the Bento Deployments use.
        # A generic range over natsDataPlane cannot: that map mixes scalars with
        # the consumer sub-maps, and enablement depends on historyStore, which
        # the sub-maps do not carry.
        gt = render(CRONJOB, "natsDataPlane.historyStore=greptimedb")
        self.assertIn("thingsflow-greptimedb-durable", gt)
        self.assertIn("thingsflow-entity-greptimedb-durable", gt)

        qdb = render(CRONJOB, "natsDataPlane.historyStore=questdb")
        self.assertNotIn("thingsflow-greptimedb-durable", qdb)
        self.assertNotIn("thingsflow-entity-greptimedb-durable", qdb)

    def test_the_probeless_alarm_materializer_durable_is_covered(self):
        # Created by the Go client, not the bootstrap hook, and living under a
        # different values key -- so a values-driven guard would miss it. Its
        # Deployment has no probes of any kind.
        rendered = render(CRONJOB)
        self.assertIn("TF_ALARMS|thingsflow-alarm-materializer", rendered)

    def test_credentials_are_declared_before_the_url_that_expands_them(self):
        # thingsflow.natsURL emits $(NATS_USER)/$(NATS_PASSWORD) for Kubernetes
        # to expand; the vars must already be declared at that point.
        rendered = render(CRONJOB, "nats.auth.enabled=true", "nats.auth.password=x")
        self.assertLess(
            rendered.index("name: NATS_USER"), rendered.index("name: NATS_URL")
        )
        # natsURLLiteral renders NO credentials under existingSecret, which is
        # exactly production, so the env-expanding form must be the one used.
        self.assertIn("$(NATS_USER)", rendered)

    def test_deadline_outlives_the_two_sample_window(self):
        # The freshness guard's 90s would be exceeded by the sleep alone, and a
        # DeadlineExceeded Job is indistinguishable from a real alert but carries
        # no metric line.
        rendered = render(CRONJOB, "monitoring.consumerGuard.sampleSeconds=120")
        self.assertIn("activeDeadlineSeconds: 300", rendered)

    def test_a_failed_tick_stays_failed_and_is_kept(self):
        rendered = render(CRONJOB)
        self.assertIn("backoffLimit: 0", rendered)
        self.assertIn("suspend: false", rendered)
        self.assertIn("concurrencyPolicy: Forbid", rendered)

    def test_no_scrape_infrastructure_is_introduced(self):
        # D-07: the failed Job IS the alert. There is no scrape target for a Job
        # stdout line.
        rendered = render(CRONJOB) + render(SCRIPTS)
        # Narrow assertions: the templates' own comments say "do NOT add a
        # ServiceMonitor", so a bare substring check matches the warning itself.
        self.assertNotIn("kind: ServiceMonitor", rendered)
        self.assertNotIn("prometheus.io/scrape:", rendered)


class OrphanDurableTest(unittest.TestCase):
    def test_disabling_questdb_removes_its_durable(self):
        # An orphaned durable is not inert on a capped, discard-old stream: the
        # questdb one grew to 106k pending and never drained. The guard would
        # alert on it forever and correctly, so it is removed at the source
        # rather than added to an ignore list.
        rendered = render("templates/nats.yaml", "natsDataPlane.questdb.enabled=false")
        self.assertIn('consumer rm "TF_RAW" "thingsflow-questdb-durable"', rendered)


class LatestKvSingleWriterTest(unittest.TestCase):
    """latest-kv must never ship with more than one replica.

    The overlays carried `replicas: 2` against the invariant documented in
    values.yaml and files/bento-nats-latest-kv.yaml -- in the example files new
    deployments copy from, so every fresh cluster inherited it. Production was
    running 1, which is why it was a landmine rather than a live fault.
    """

    def test_every_shipped_values_file_keeps_the_single_writer(self):
        import yaml

        # values-cluster.yaml is gitignored (the tracked template is
        # values-cluster.example.yaml), so it is deliberately not listed here --
        # a test may only depend on files the repo actually ships.
        for name in sorted(CHART.glob("values*.yaml")):
            with self.subTest(values=name.name):
                doc = yaml.safe_load(name.read_text()) or {}
                latest = (doc.get("natsDataPlane") or {}).get("latestKv") or {}
                if "replicas" not in latest:
                    continue
                self.assertEqual(
                    latest["replicas"],
                    1,
                    f"{name.name} scales the single-writer doc-merge consumer",
                )

    def test_the_rendered_deployment_is_a_single_writer(self):
        import yaml

        rendered = render("templates/nats-data-plane-bento.yaml")
        deploys = {
            d["metadata"]["name"]: d
            for d in yaml.safe_load_all(rendered)
            if d and d.get("kind") == "Deployment"
        }
        latest = next(n for n in deploys if n.endswith("nats-latest-kv"))
        self.assertEqual(deploys[latest]["spec"]["replicas"], 1)


if __name__ == "__main__":
    unittest.main()
