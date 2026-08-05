"""The public tree must not silently accumulate vendored or heavyweight artifacts.

Why this exists: the project's founding incident is a store that grew without
bound because nothing enforced a limit. The repository itself is a store. As of
2026-08-05 the tracked tree is clean — zero `.venv/`/`__pycache__`/`.pyc`
entries, and the largest tracked file is 348,819 bytes
(flow-core/tb-resources/dashboards/gateways_dashboard.json). A 12MB local
`.venv` and hundreds of raw benchmark results exist on disk but are correctly
untracked. This test pins that state so it cannot regress silently: a stray
`git add` of a virtualenv, a bytecode cache, or a multi-megabyte result dump
fails the OSS release gate instead of shipping to the public repo.

The size ceiling is enforced with an explicit allowlist, deliberately empty
today. Tracking a file larger than 500KB is sometimes legitimate (a curated
dashboard, a piece of benchmark evidence) — but it must be a conscious decision
that shows up in a PR diff as an allowlist entry, never an accident.
"""
import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]

# Ceiling for any tracked file, in bytes. The largest legitimate tracked file
# today is 348,819 bytes, so 500KB passes with room while still catching result
# dumps and vendored blobs. Raise ONLY by allowlisting the specific path below.
MAX_TRACKED_BYTES = 500_000

# Paths (repo-relative, as printed by `git ls-files`) that are consciously
# allowed to exceed MAX_TRACKED_BYTES. Empty on purpose: every entry added here
# is a reviewed decision visible in the diff, not a silent default.
SIZE_ALLOWLIST: set = set()


def _tracked_files():
    """Returns every git-tracked path, NUL-split so paths with spaces survive."""
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    ).stdout
    return [p.decode("utf-8") for p in out.split(b"\0") if p]


def _is_vendored_artifact(path):
    """True for virtualenvs, bytecode caches, and compiled Python files."""
    return (
        path.startswith(".venv/")
        or "/.venv/" in path
        or "__pycache__" in path
        or path.endswith(".pyc")
    )


class RepoHygieneTest(unittest.TestCase):
    def test_no_vendored_python_artifacts(self):
        """Virtualenvs and bytecode belong to the machine, not the tree."""
        offenders = [p for p in _tracked_files() if _is_vendored_artifact(p)]
        self.assertEqual(
            offenders, [],
            f"tracked vendored Python artifacts found: {offenders} — these are "
            f"machine-local (.gitignore already excludes **/.venv/ and "
            f"**/__pycache__/); `git rm -r --cached` them instead of shipping "
            f"them to the public repo.",
        )

    def test_no_oversized_tracked_files(self):
        """A file that outgrows the ceiling must be allowlisted consciously."""
        offenders = []
        for path in _tracked_files():
            if path in SIZE_ALLOWLIST:
                continue
            try:
                size = (ROOT / path).stat().st_size
            except FileNotFoundError:
                # Tracked but absent from the working tree (mid-rename, dirty
                # state): nothing to measure, and not this test's concern.
                continue
            if size > MAX_TRACKED_BYTES:
                offenders.append(f"{path} ({size:,} bytes)")
        self.assertEqual(
            offenders, [],
            f"tracked files exceed {MAX_TRACKED_BYTES:,} bytes: {offenders}. "
            f"If a file this large truly belongs in the public tree, add its "
            f"exact path to SIZE_ALLOWLIST in this test so the decision is "
            f"reviewed in the PR diff; otherwise keep it untracked (see "
            f"benchmarks/README.md 'Artifact policy').",
        )

    def test_gitignore_keeps_venv_pattern(self):
        """The .gitignore line that keeps venvs out must itself stay present."""
        lines = (ROOT / ".gitignore").read_text().splitlines()
        self.assertIn(
            "**/.venv/", lines,
            ".gitignore no longer contains the '**/.venv/' pattern — without "
            "it a local virtualenv shows up as thousands of untracked files "
            "one `git add -A` away from the public repo.",
        )


if __name__ == "__main__":
    unittest.main()
