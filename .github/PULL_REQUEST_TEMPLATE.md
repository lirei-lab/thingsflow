## What & why

<!-- 1-2 lines: what changes, and the problem it solves. -->

## Before requesting review

- [ ] If you touched `flow-core/`: from `flow-core/`, `gofmt -l .` (must print
      nothing — it is CI's first gate) then `go vet ./... && go test -p 1 -count=1 ./...`
      (see CONTRIBUTING.md for why the suite is serial)
- [ ] Contract tests pass: `python3 -m unittest discover -s tools/python -p 'test_*.py'`
- [ ] Release gate passes: `bash tools/check-oss-release.sh`
- [ ] If you touched the stack or data plane (`docker/`, chart, Bento/NATS config),
      optionally run the compose smoke locally:
      `docker compose -f docker/docker-compose-nats.yml up -d --build`
      then `bash tools/smoke/fresh-install-smoke.sh`
- [ ] No private hostnames, IPs, kubeconfigs, or secrets anywhere in the diff —
      the release gate greps for them and fails the PR
- [ ] Measured, not guessed: any performance or resource claim in this PR
      carries its evidence (numbers, logs, or a test)

---

CI runs `oss-release-gate` on every PR; `test-flow-core`, `lint-migrations`,
`fresh-install-smoke`, and `docs` also run automatically when your changed
paths match their filters (see the CI gates table in CONTRIBUTING.md).
