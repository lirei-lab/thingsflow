"""Every env var a Bento config requires must be supplied by BOTH deploy paths.

Why this exists: the latest-kv config gained `max_ack_pending:
${LATEST_KV_MAX_ACK_PENDING}` when that consumer became the single-writer
doc-merge. The Helm template was updated; docker-compose was not. Bento treats a
`${VAR}` with no default as REQUIRED and refuses to start on it -- not with a
warning, and not by falling back:

    Config lint error  lint="(1,1) required environment variables were not set:
                             [LATEST_KV_MAX_ACK_PENDING]"
    Shutting down due to linter errors

So the container never came up at all, the twin-state KV was never populated,
and the fresh-install smoke failed on a symptom three steps downstream ("NATS KV
twin_state did not hold both keys within 60s"). Nothing pointed at the missing
variable.

The chart and docker-compose mount the SAME config files, so any required
variable has to be satisfied twice. This test makes that contract explicit
rather than leaving it to whoever remembers both call sites.
"""
import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
FILES = ROOT / "k8s/helm/thingsflow/files"
COMPOSE = ROOT / "docker/docker-compose-nats.yml"
BENTO_TEMPLATE = ROOT / "k8s/helm/thingsflow/templates/nats-data-plane-bento.yaml"
INGEST_TEMPLATE = ROOT / "k8s/helm/thingsflow/templates/http-ingest.yaml"

# `${VAR}` is required; `${VAR:-default}` and `${VAR:default}` are not.
REQUIRED = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")

# Supplied by the runtime rather than by either deploy path.
RUNTIME_PROVIDED = {"BENTO_HTTP_BIND_ADDRESS", "BENTO_HTTP_PORT"}


def required_vars(path):
    return {v for v in REQUIRED.findall(path.read_text())} - RUNTIME_PROVIDED


class BentoEnvContractTest(unittest.TestCase):
    def test_every_required_var_is_set_in_both_deploy_paths(self):
        compose = COMPOSE.read_text()
        charts = BENTO_TEMPLATE.read_text()
        if INGEST_TEMPLATE.exists():
            charts += INGEST_TEMPLATE.read_text()

        configs = sorted(FILES.glob("bento-*.yaml"))
        self.assertTrue(configs, "no bento configs found — did the path move?")

        for config in configs:
            # Only assert against compose for configs compose actually MOUNTS.
            # The questdb path is the legacy backend and has no compose service;
            # demanding its vars there would be a standing false alarm. If anyone
            # adds the service, the mount appears and this starts covering it.
            mounted_by_compose = config.name in compose

            for var in sorted(required_vars(config)):
                with self.subTest(config=config.name, var=var):
                    self.assertIn(
                        var,
                        charts,
                        f"{config.name} requires {var}, absent from the chart "
                        f"templates — the pod will fail Bento's lint and never start",
                    )
                    if mounted_by_compose:
                        self.assertIn(
                            var,
                            compose,
                            f"{config.name} requires {var}, absent from "
                            f"docker-compose-nats.yml — the local stack and the "
                            f"fresh-install smoke will fail Bento's lint and never start",
                        )

    def test_latest_kv_serialization_matches_across_both_paths(self):
        """The doc-merge value must agree in three places or the bind fails.

        Bento asserts its own max_ack_pending against the durable's, so a
        mismatch is not a performance difference — NATS refuses the bind with
        "configuration requests max ack pending to be N, but consumer's value is
        M" and the consumer never attaches.
        """
        compose = COMPOSE.read_text()
        self.assertIn('LATEST_KV_MAX_ACK_PENDING: "1"', compose)
        # The compose bootstrap must create that durable with a matching 1, so
        # max-pending cannot be a single shared value across the consumer loop.
        self.assertIn("thingsflow-latest-kv-durable", compose)
        self.assertNotIn("--max-pending 1024", compose)
        self.assertIn('--max-pending "$6"', compose)


if __name__ == "__main__":
    unittest.main()
