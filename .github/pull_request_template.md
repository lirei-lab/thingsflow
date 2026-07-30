<!--
  Quick guide for what makes a PR easy to merge.
  Delete the sections that don't apply, keep what does.
-->

## What this PR changes

<one or two sentences — what's different after this lands?>

## Why

<the problem this PR solves; link to issue / runbook / incident if relevant>

## Verification

Gates from [CONTRIBUTING.md](../CONTRIBUTING.md):

- [ ] `cd flow-core && gofmt -l .` prints nothing
- [ ] `cd flow-core && go vet ./...`
- [ ] `cd flow-core && go build ./...`
- [ ] `cd flow-core && go test -p 1 -count=1 ./...`
- [ ] `python3 -m unittest discover -s tools/python -p 'test_*.py'` (chart/platform contract tests)
- [ ] `bash tools/check-oss-release.sh`
- [ ] `bash tools/smoke-local.sh` (if the change touches the runtime path)
- [ ] `helm lint k8s/helm/thingsflow` and `helm template k8s/helm/thingsflow` (if the chart was edited)
- [ ] Manual UI/API check: <screenshot or curl output>

## Notes for reviewers

<anything reviewers should look at first; a known caveat;
 a follow-up PR you intend to open>
