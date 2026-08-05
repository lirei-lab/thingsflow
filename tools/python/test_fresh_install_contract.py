"""INSTALL.md's fresh-install commands must stay identical to what CI executes.

Why this exists: docs/INSTALL.md tells a new user to run two exact commands,
and the Fresh-install smoke workflow runs the same two against a from-scratch
stack. Nothing else ties the doc to the workflow — the smoke workflow's path
filters deliberately exclude docs/ (a doc edit should not cost a 15-minute
stack build). This test is the tie: if either the doc or the contract files
drift, the OSS release gate goes red instead of the drift shipping silently.
"""
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]

# The two commands a new user copies, verbatim. The smoke script is the source
# of truth; the doc and the workflow both carry these exact strings.
UP_COMMAND = "docker compose -f docker/docker-compose-nats.yml up -d --build"
SMOKE_COMMAND = "bash tools/smoke/fresh-install-smoke.sh"


class FreshInstallContractTest(unittest.TestCase):
    def setUp(self):
        self.install = (ROOT / "docs/INSTALL.md").read_text()

    def test_install_doc_carries_the_exact_contract_commands(self):
        for cmd in (UP_COMMAND, SMOKE_COMMAND):
            self.assertIn(
                cmd, self.install,
                f"docs/INSTALL.md no longer contains the exact command "
                f"{cmd!r}. The fresh-install smoke is the source of truth — "
                f"align the doc with tools/smoke/fresh-install-smoke.sh and "
                f".github/workflows/fresh-install-smoke.yml, or update all "
                f"three together.",
            )

    def test_contract_files_exist_and_are_wired(self):
        smoke = ROOT / "tools/smoke/fresh-install-smoke.sh"
        workflow = ROOT / ".github/workflows/fresh-install-smoke.yml"
        self.assertTrue(smoke.is_file(), "fresh-install smoke script missing")
        self.assertTrue(workflow.is_file(), "fresh-install smoke workflow missing")
        workflow_text = workflow.read_text()
        # Symmetric pin: the doc-side check above catches doc drift; these
        # catch workflow drift, so "the commands the workflow runs" stays true
        # from both directions.
        for cmd in (UP_COMMAND, SMOKE_COMMAND):
            self.assertIn(
                cmd, workflow_text,
                f"the workflow no longer runs the exact command {cmd!r} that "
                f"docs/INSTALL.md promises — update doc, workflow, and this "
                f"test together.",
            )


if __name__ == "__main__":
    unittest.main()
