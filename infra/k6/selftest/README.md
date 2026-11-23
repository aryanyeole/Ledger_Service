# Cache benchmark self-test

`stub_api.py` (Python standard library only) serves the API surface `infra/k6/cache-benchmark.js` uses, with exact
balances: an instrument check for the workload and the stale judge, never a result (D-146). From the repo root:

```powershell
# exact: every read correct -> 0 stale reads, k6 exit 0
$env:MODE = "exact"; $stub = Start-Process python -ArgumentList "infra\k6\selftest\stub_api.py","18080" -PassThru -NoNewWindow; Remove-Item Env:MODE
docker run --rm -v "${PWD}\infra\k6:/scripts:ro" -e API_KEY=stub-key -e BASE_URL=http://host.docker.internal:18080 -e VUS=2 -e DURATION=15s grafana/k6:2.3.0 run /scripts/cache-benchmark.js; "k6 exit $LASTEXITCODE"
taskkill /F /T /PID $stub.Id

# stale (its own port, so it never races the exact stub's shutdown): every 7th balance read served 0.01 high ->
# stale hits, misses and bypasses counted, thresholds fail, k6 exit 99
$env:MODE = "stale"; $stub = Start-Process python -ArgumentList "infra\k6\selftest\stub_api.py","18081" -PassThru -NoNewWindow; Remove-Item Env:MODE
docker run --rm -v "${PWD}\infra\k6:/scripts:ro" -e API_KEY=stub-key -e BASE_URL=http://host.docker.internal:18081 -e VUS=2 -e DURATION=15s grafana/k6:2.3.0 run /scripts/cache-benchmark.js; "k6 exit $LASTEXITCODE"
taskkill /F /T /PID $stub.Id
```

A judged read that gets no response (k6 status 0, e.g. `dial: i/o timeout` against this single-process stub) counts as
`judged_incomplete`; against the real stack that makes a run invalid (§8.5), and it is rerun.
