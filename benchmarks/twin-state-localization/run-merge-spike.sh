#!/usr/bin/env bash
# Merge-writer spike orchestrator — Milestone 4 Phase 1.
#
# STATUS (2026-08-17, live evidence): the config-only CAS merge write path is
# INFEASIBLE. cas-feasibility-probe.sh proved that Bento 1.8.1 exposes NO KV
# revision/sequence from either the nats_kv cache get (returns value bytes
# only) or a nats_jetstream input (nats_subject_sequence/nats_sequence are
# "none"), so the Nats-Expected-Last-Subject-Sequence CAS header cannot be
# sourced config-only. The CAS ENFORCEMENT half works (a stale expected-seq
# publish is rejected by the KV backing stream — CASREJECT=PASS), but without
# a config-only revision source the optimistic-CAS merge loop (read -> merge ->
# CAS write -> retry-on-conflict) cannot be implemented in pure Bento config.
#
# Therefore there is NO merge-doc-writer.yaml to orchestrate. This runner
# exists to (a) record that gate outcome deterministically and (b) be the
# vehicle for re-running the live $KV. whole-doc semantics verifier
# (verify-kv-doc-publish-semantics.sh), which IS meaningful without the merge
# writer and documents the CAS enforcement half.
#
# Usage:
#   run-merge-spike.sh            # runs the 4-check whole-doc semantics verifier
#   run-merge-spike.sh --verify   # same, explicit
#
# Prints:
#   MERGE_WRITER=INFEASIBLE
#   then the READBACK/WATCHER/WRITETWICE/CASREJECT lines from the verifier.
# Exits 0 iff the verifier's four checks all PASS.
#
# NEVER touches the production Helm release or the production latest-kv durable.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "MERGE_WRITER=INFEASIBLE"
echo "-- reason: Bento 1.8.1 exposes no KV revision/sequence from cache get or JS input; see cas-feasibility-probe.sh + 01-01-SUMMARY.md" >&2

if [[ "${1:-}" == "--verify" || "${1:-}" == "" ]]; then
  echo "-- running whole-doc \$KV. semantics verifier (CAS-enforcement half)" >&2
  exec "$HERE/verify-kv-doc-publish-semantics.sh"
fi

echo "usage: $0 [--verify]" >&2
exit 2
