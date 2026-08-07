"""The freshness guard must separate a halted writer from an idle platform.

Why this exists: the guard alerted whenever the telemetry table held rows but
none were recent. That is the signature of a real halt, but it is equally the
signature of a platform where nobody happens to be publishing -- an evaluation
cluster after a load test, a pilot overnight, a fleet between duty cycles. On an
idle test cluster it produced a failed Job every five minutes indefinitely, which
is the same way it used to fail on every fresh install, and a guard that always
alerts is one operators learn to ignore.

The discriminator is whether there is work waiting. The incident this guard was
written for -- a dead NATS subscription while every pod stayed Running and the
edge kept returning 200 -- leaves messages piling up on the history consumer with
nothing draining them. An idle platform has an empty backlog because nothing was
published.

Verified in cluster across all three branches before this test was written:
backlog 0 with 3,240,000 historical rows exits 0; the writer scaled to zero with
4,000 messages waiting exits 1 and names the count; an unreadable probe exits 1.
"""
import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
CHART = ROOT / "k8s/helm/thingsflow"
TEMPLATE = "templates/greptimedb-freshness-guard.yaml"


def render(*set_args):
    cmd = ["helm", "template", "tf", str(CHART), "--show-only", TEMPLATE]
    for arg in set_args:
        cmd += ["--set", arg]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if out.returncode != 0:
        raise AssertionError(f"helm template failed: {out.stderr}")
    return out.stdout


class FreshnessGuardBacklogTest(unittest.TestCase):
    def setUp(self):
        self.rendered = render()

    def test_the_probe_can_never_fail_the_job(self):
        """An init container that fails takes the whole Job down with it.

        If the probe exited non-zero when NATS was briefly unreachable, the Job
        would fail — and a failed Job IS this guard's alert surface, so a
        transient blip would be indistinguishable from a halted data plane.
        Failure is encoded in the file the guard reads, never in the exit code.
        """
        probe = self.rendered.split("initContainers:", 1)[1].split("containers:", 1)[0]
        self.assertIn(
            'echo "unknown" > /probe/backlog', probe,
            "the probe must record failure in the file rather than by exiting.",
        )
        self.assertNotIn(
            "exit 1", probe,
            "the probe must not exit non-zero: an init container failure fails "
            "the Job, which this guard reports as a data-plane halt.",
        )

    def test_an_unreadable_backlog_alerts_rather_than_staying_quiet(self):
        """The fail-safe direction is the whole point.

        Not being able to read the backlog is not evidence of health. Treating
        `unknown` as "probably fine" would reintroduce exactly the silent blind
        spot the guard was written for: the original incident ran ~14h with every
        pod Running and every response a 200.
        """
        body = self.rendered
        marker = 'if [ "${BACKLOG}" = "unknown" ]; then'
        self.assertIn(marker, body, "the unknown case must be handled explicitly.")
        branch = body.split(marker, 1)[1].split("fi", 1)[0]
        self.assertIn(
            "exit 1", branch,
            "an unreadable backlog must alert; silence here is the blind spot "
            "this guard exists to close.",
        )

    def test_an_empty_backlog_with_stale_rows_is_not_an_alert(self):
        """Idle is a normal state, not a fault."""
        marker = 'if [ "${BACKLOG}" = "0" ]; then'
        self.assertIn(marker, self.rendered)
        branch = self.rendered.split(marker, 1)[1].split("fi", 1)[0]
        self.assertIn("exit 0", branch)
        self.assertIn("thingsflow_greptimedb_write_stale 0", branch)

    def test_waiting_work_still_alerts_and_reports_how_much(self):
        """The regression that would matter most is losing the halt signal."""
        self.assertIn("data-plane ingest is halted", self.rendered)
        self.assertIn("${BACKLOG} message(s) are waiting", self.rendered)

    def test_the_backlog_counts_undelivered_and_unacked(self):
        """A consumer that dies mid-flight leaves messages already delivered.

        Counting only num_pending would miss precisely that shape of halt, which
        is the one the original incident had.
        """
        self.assertIn("(.num_pending // 0) + (.num_ack_pending // 0)", self.rendered)

    def test_the_probe_authenticates_when_nats_auth_uses_an_existing_secret(self):
        """The bug this catches only appears on installs that enable NATS auth.

        The probe first used `thingsflow.natsURLLiteral`, which embeds a password
        only when one is a literal value in values.yaml. An install using
        `nats.auth.existingSecret` — which is the production configuration —
        renders a URL with no credentials at all, so `consumer info` fails, the
        probe reports an unreadable backlog, and this guard alerts on every tick.
        It would have passed every test against a local install with auth off.

        The credentials must also come BEFORE `NATS_URL` in the env list:
        Kubernetes only substitutes variables declared earlier, so a URL declared
        first keeps the literal `$(NATS_USER)` text and fails auth.
        """
        rendered = render(
            "nats.auth.enabled=true",
            "nats.auth.existingSecret.name=thingsflow-nats-auth",
        )
        probe = rendered.split("initContainers:", 1)[1].split("\n          containers:", 1)[0]

        self.assertIn(
            "$(NATS_USER):$(NATS_PASSWORD)@", probe,
            "with auth enabled the probe's NATS_URL carries no credentials; it "
            "cannot read the consumer and will alert on every tick.",
        )
        self.assertLess(
            probe.index("NATS_PASSWORD"), probe.index("name: NATS_URL"),
            "NATS_URL is declared before the credentials, so Kubernetes leaves "
            "the literal $(NATS_USER) in the URL and auth fails.",
        )

    def test_the_probe_uses_an_image_the_chart_already_pins(self):
        """The psql image has no HTTP client at all — no curl, wget or python.

        Checked in cluster rather than assumed. nats-box is already pinned by
        this chart and carries both the nats CLI and jq, so the probe adds no new
        image to the supply chain.
        """
        self.assertIn("natsio/nats-box:", self.rendered)

    def test_the_guard_still_stays_quiet_on_a_fresh_install(self):
        """The earlier fix must survive this one."""
        self.assertIn("the platform has never received data", self.rendered)


if __name__ == "__main__":
    unittest.main()
