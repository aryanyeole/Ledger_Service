<#
.SYNOPSIS
The M5 cache benchmark runner (D-146, docs/design/cache-invalidation.md §8). Harness only: no numbers live here.

.DESCRIPTION
Compose runs postgres, kafka and redis only. Per mode and run, in this order:
  1. flush Redis (nothing else is reset; every run seeds fresh accounts);
  2. start the ledger-api jar with the mode's settings and wait for /actuator/health UP (Flyway has migrated):
       A          balance ttl 60 s, invalidation on in both services
       B          balance ttl 1 s, ledger.cache.balance.invalidation=false and WORKER_BALANCE_INVALIDATION=0
       C-stopped  as A, Redis stopped (docker compose stop redis) for -OutageSeconds from -OutageStartSeconds
       C-paused   as A, Redis paused (docker compose pause redis) for the same window
  3. wait until the relay has created ledger.payments.v1 and the DLQ topic;
  4. with the worker not running, commit the bench-settlement-worker group at ledger.payments.v1's current end on
     every partition (the D-123 pattern), so the group always exists and each run sees only its own events;
  5. start the worker; mode guard (D-146): refuse to run k6 unless the API's log has the "Balance cache invalidation
     is OFF" WARNING iff the mode is B, and worker_started.balance_invalidation is "off" for B, "on" otherwise;
  6. wait until the group's lag is 0 (a check: the reset made it so);
  7. record hardware and versions (D-024), Redis evicted_keys and both services' cache counters, run k6
     (grafana/k6:2.3.0, through host.docker.internal), and record them again;
  8. judge validity: an evicted_keys delta or any judged_incomplete makes the run INVALID (§8.5); k6's thresholds fail
     it on any stale miss or stale bypass (both read the DB); mode B must show stale_hit > 0 (positive control);
  9. cleanup, whatever happened: stop the worker and the jar (process trees), then restore Redis only if this run
     stopped or paused it, each step with its own error handling; then assert no bench-launched process is alive.
Raw outputs go to <OutputDir>/<mode>-<run>/ (default docs/load-tests/raw/m5). Only the small evidence files there are
committed (k6-summary.json, counters.json, env.json, validity.json, redis-info-*.txt); .gitignore keeps out the logs,
stderr and k6's stdout. Every failure names the step it happened in.

Credentials: LEDGER_API_APP_PASSWORD, LEDGER_API_OWNER_PASSWORD and SETTLEMENT_WORKER_PASSWORD come from the
environment if all are set, else from .env. Values are never printed or written; only their source is logged.
The bench API key is generated per run: only its SHA-256 reaches the jar, and the key reaches only k6.

.EXAMPLE
.\scripts\bench-cache.ps1 -Modes A,B,C-stopped,C-paused -Runs 3 -DurationSeconds 300 -Vus 20 -Seed 42
.\scripts\bench-cache.ps1 -Modes A -Runs 1 -DurationSeconds 30 -Vus 2 -OutputDir "$env:TEMP\ledger-bench-smoke"    # a smoke, outside the repo
#>
param(
    [string[]] $Modes = @("A", "B", "C-stopped", "C-paused"),
    [int] $Runs = 3,
    [int] $DurationSeconds = 300,
    [int] $Vus = 20,
    [int] $Seed = 42,
    [int] $CustomersPerVu = 3,
    [int] $OutageStartSeconds = 60,
    [int] $OutageSeconds = 30,
    [int] $ApiPort = 8080,
    [int] $WorkerMetricsPort = 8001,
    [int] $LagTimeoutSeconds = 120,
    [int] $TopicTimeoutSeconds = 60,
    [string] $Weights = "deposit=10,authorize=15,ownedRead=50,foreignRead=25",
    [int] $DeclinePercent = 20,
    [string] $OutputDir = "docs\load-tests\raw\m5"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$K6Image = "grafana/k6:2.3.0"
$WorkerGroup = "bench-settlement-worker"
$Topic = "ledger.payments.v1"
$DlqTopic = "ledger.payments.dlq.v1"
$KafkaTools = "/opt/kafka/bin"
$KafkaInternal = "localhost:29092"
# Every dependency of the host-run services by 127.0.0.1, never localhost: Compose publishes on 127.0.0.1 only (D-029),
# and Windows resolves localhost to ::1 first. redis-py's 200 ms connect timeout expires on ::1, so every worker
# invalidation failed (found in T14's smoke); for Postgres and Kafka, Windows takes ~2 s to refuse a closed ::1 port
# before falling back, which would at best add connect latency to the measurement. One limit: the broker advertises
# EXTERNAL://localhost:9092 (docker-compose.yml), so after bootstrapping here a host Kafka client reconnects to localhost.
$RedisUrl = "redis://127.0.0.1:6379"
$PostgresHost = "127.0.0.1"
$KafkaBootstrap = "127.0.0.1:9092"
$Jar = Join-Path $Root "services\ledger-api\target\ledger-api-0.0.1-SNAPSHOT.jar"
$WorkerDir = Join-Path $Root "services\settlement-worker"
$WorkerPython = Join-Path $WorkerDir ".venv\Scripts\python.exe"
$Secrets = @("LEDGER_API_APP_PASSWORD", "LEDGER_API_OWNER_PASSWORD", "SETTLEMENT_WORKER_PASSWORD")

function Log([string] $message) { Write-Host ("[bench] " + $message) }

# Runs a native command without letting its stderr become a terminating error (Windows PowerShell 5.1 turns a
# 2>&1-captured stderr line into an ErrorRecord, which "Stop" makes fatal). Throws, naming the step, only on a non-zero
# exit, unless -AllowFailure.
function Invoke-Native([string] $Step, [string] $Exe, [string[]] $Arguments, [switch] $AllowFailure) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = @(& $Exe @Arguments 2>&1 | ForEach-Object { "$_" })
        $code = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $previous }
    if ($code -ne 0 -and -not $AllowFailure) {
        throw "$Step failed (exit $code): $(($out | Select-Object -Last 5) -join ' | ')"
    }
    return [pscustomobject]@{ Code = $code; Output = $out }
}

function Compose([string] $Step, [string[]] $Arguments, [switch] $AllowFailure) {
    return Invoke-Native $Step "docker" (@("compose") + $Arguments) -AllowFailure:$AllowFailure
}

function Get-Credentials {
    $fromEnv = $true
    foreach ($name in $Secrets) { if (-not (Test-Path "Env:$name")) { $fromEnv = $false } }
    $values = @{}
    if ($fromEnv) {
        foreach ($name in $Secrets) { $values[$name] = (Get-Item "Env:$name").Value }
        Log "credentials from environment"
    }
    else {
        $envFile = Join-Path $Root ".env"
        if (-not (Test-Path $envFile)) { throw "set $($Secrets -join ', ') in the environment or create .env" }
        foreach ($name in $Secrets) {
            $match = Select-String -Path $envFile -Pattern ("^" + $name + "=(.+)$") | Select-Object -First 1
            if ($null -eq $match) { throw "$name is missing from .env" }
            $values[$name] = $match.Matches[0].Groups[1].Value
        }
        Log "credentials from .env"
    }
    return $values
}

function Test-PortFree([int] $port) {
    $client = New-Object System.Net.Sockets.TcpClient
    try { $client.Connect("127.0.0.1", $port); return $false } catch { return $true } finally { $client.Close() }
}

# A started process plus its handle, read at once so ExitCode survives (Start-Process -PassThru loses it otherwise).
function Start-Bench([string] $FilePath, [string[]] $Arguments, [string] $Out, [string] $Err, [string] $Dir) {
    $p = $(if ($Dir) {
            Start-Process -FilePath $FilePath -ArgumentList $Arguments -WorkingDirectory $Dir -RedirectStandardOutput $Out -RedirectStandardError $Err -PassThru -NoNewWindow
        } else {
            Start-Process -FilePath $FilePath -ArgumentList $Arguments -RedirectStandardOutput $Out -RedirectStandardError $Err -PassThru -NoNewWindow
        })
    $null = $p.Handle
    return $p
}

# Every process in a tree (the venv's python.exe is a launcher with the interpreter as a child).
function Get-TreeIds([int] $rootId) {
    $ids = @($rootId)
    $frontier = @($rootId)
    while ($frontier.Count -gt 0) {
        $next = @()
        foreach ($id in $frontier) {
            $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$id" -ErrorAction SilentlyContinue | ForEach-Object { [int] $_.ProcessId })
            $next += $children
        }
        $ids += $next
        $frontier = $next
    }
    return $ids
}

function Stop-BenchTree([string] $label, $process) {
    if ($null -eq $process) { return @() }
    $ids = @(Get-TreeIds $process.Id)
    Invoke-Native "stop $label" "taskkill" @("/F", "/T", "/PID", "$($process.Id)") -AllowFailure | Out-Null
    return $ids
}

function Get-AliveIds([int[]] $ids) {
    return @($ids | Where-Object { $null -ne (Get-Process -Id $_ -ErrorAction SilentlyContinue) })
}

function New-ApiKey {
    $bytes = New-Object byte[] 24
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $key = [Convert]::ToBase64String($bytes).TrimEnd("=").Replace("+", "-").Replace("/", "_")
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $hash = -join ($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($key)) | ForEach-Object { $_.ToString("x2") })
    return @{ Key = $key; Hash = $hash }
}

function Invoke-Api([string] $path, [string] $key) {
    return Invoke-RestMethod -Uri ("http://127.0.0.1:" + $ApiPort + $path) -Headers @{ "X-Api-Key" = $key } -TimeoutSec 10
}

function Get-ApiCounter([string] $name, [string] $result, [string] $key) {
    try {
        $metric = Invoke-Api ("/actuator/metrics/" + $name + "?tag=result:" + $result) $key
        return [decimal] $metric.measurements[0].value
    }
    catch { return [decimal] 0 }  # a counter with no samples yet
}

function Get-Counters([string] $key) {
    $api = [ordered]@{}
    foreach ($result in "hit", "miss", "bypass", "unparseable") { $api["ledger.balance.cache.read." + $result] = Get-ApiCounter "ledger.balance.cache.read" $result $key }
    foreach ($result in "written", "stale_token", "unavailable") { $api["ledger.balance.cache.populate." + $result] = Get-ApiCounter "ledger.balance.cache.populate" $result $key }
    foreach ($result in "sent", "failed") { $api["ledger.balance.cache.invalidation." + $result] = Get-ApiCounter "ledger.balance.cache.invalidation" $result $key }
    foreach ($result in "hit", "miss", "bypass") { $api["ledger.idempotency.cache.read." + $result] = Get-ApiCounter "ledger.idempotency.cache.read" $result $key }
    $worker = [ordered]@{}
    $text = (Invoke-WebRequest -Uri ("http://127.0.0.1:" + $WorkerMetricsPort + "/metrics") -UseBasicParsing -TimeoutSec 10).Content
    foreach ($result in "sent", "failed") {
        $line = ($text -split "`n") | Where-Object { $_ -like ('settlement_balance_invalidations_total{result="' + $result + '"}*') } | Select-Object -First 1
        $worker["settlement_balance_invalidations_total." + $result] = $(if ($line) { [decimal] ($line -split " ")[-1] } else { [decimal] 0 })
    }
    return [ordered]@{ api = $api; worker = $worker }
}

function Get-EvictedKeys {
    $info = (Compose "redis INFO stats" @("exec", "-T", "redis", "redis-cli", "INFO", "stats")).Output
    $line = $info | Where-Object { $_ -like "evicted_keys:*" } | Select-Object -First 1
    return [long] ($line.Split(":")[1].Trim())
}

function Wait-Topics {
    $deadline = (Get-Date).AddSeconds($TopicTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $list = (Compose "list topics" @("exec", "-T", "kafka", "$KafkaTools/kafka-topics.sh", "--bootstrap-server", $KafkaInternal, "--list") -AllowFailure).Output
        if (($list -contains $Topic) -and ($list -contains $DlqTopic)) { return }
        Start-Sleep -Seconds 1
    }
    throw "the relay didn't create $Topic and $DlqTopic within $TopicTimeoutSeconds s"
}

# D-123's pattern: with no member running, commit the group at the topic's end on every partition. Creates the group
# on the first run; later runs skip whatever earlier runs (or anything else) left on the topic.
function Reset-GroupToEnd {
    Compose "reset $WorkerGroup to the end of $Topic" @("exec", "-T", "kafka", "$KafkaTools/kafka-consumer-groups.sh", "--bootstrap-server", $KafkaInternal,
        "--group", $WorkerGroup, "--topic", $Topic, "--reset-offsets", "--to-latest", "--execute") | Out-Null
}

function Get-Lags {
    $out = (Compose "describe $WorkerGroup" @("exec", "-T", "kafka", "$KafkaTools/kafka-consumer-groups.sh", "--bootstrap-server", $KafkaInternal,
            "--describe", "--group", $WorkerGroup) -AllowFailure).Output
    $rows = @($out | Where-Object { $_ -match ("^" + [regex]::Escape($WorkerGroup) + "\s+" + [regex]::Escape($Topic) + "\s") })
    return @($rows | ForEach-Object { ($_ -split "\s+")[5] })
}

function Wait-Lag0 {
    $deadline = (Get-Date).AddSeconds($LagTimeoutSeconds)
    $lags = @()
    while ((Get-Date) -lt $deadline) {
        $lags = Get-Lags
        if ($lags.Count -gt 0 -and -not ($lags | Where-Object { $_ -ne "0" })) { return }
        Start-Sleep -Seconds 2
    }
    throw "the $WorkerGroup group's lag didn't reach 0 within $LagTimeoutSeconds s (last: $($lags -join ','))"
}

function Get-LastEvent([string] $log) {
    $line = Get-Content $log -ErrorAction SilentlyContinue | Where-Object { $_.Trim() } | Select-Object -Last 1
    if (-not $line) { return "(no log line)" }
    try {
        $e = $line | ConvertFrom-Json
        $detail = @($e.reason, $e.error) | Where-Object { $_ } | Select-Object -First 1
        return ($e.event + $(if ($detail) { ": " + $detail } else { "" }))
    }
    catch { return $line }
}

function Wait-Health([string] $key, $process, [string] $log) {
    $deadline = (Get-Date).AddSeconds(120)
    while ((Get-Date) -lt $deadline) {
        if ($process.HasExited) { throw "ledger-api exited ($($process.ExitCode)); see $log" }
        try { if ((Invoke-Api "/actuator/health" $key).status -eq "UP") { return } } catch { }
        Start-Sleep -Milliseconds 500
    }
    throw "ledger-api not healthy within 120 s; see $log"
}

function Wait-WorkerStarted($process, [string] $log) {
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        $line = Get-Content $log -ErrorAction SilentlyContinue | Where-Object { $_ -match '"event": "worker_started"' } | Select-Object -First 1
        if ($line) { return ($line | ConvertFrom-Json) }
        if ($process.HasExited) { throw "the worker exited ($($process.ExitCode)); last event: $(Get-LastEvent $log)" }
        Start-Sleep -Milliseconds 500
    }
    throw "no worker_started within 60 s; last event: $(Get-LastEvent $log)"
}

function Get-Environment {
    $cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
    $os = Get-CimInstance Win32_OperatingSystem
    $docker = ((Invoke-Native "docker info" "docker" @("info", "--format", "{{.NCPU}} {{.MemTotal}} {{.ServerVersion}}")).Output -join " ").Trim()
    $parts = $docker -split " "
    return [ordered]@{
        commit = ((Invoke-Native "git rev-parse" "git" @("-C", $Root, "rev-parse", "HEAD")).Output -join "").Trim()
        k6 = ((Invoke-Native "k6 version" "docker" @("run", "--rm", $K6Image, "version")).Output -join " ").Trim()
        cpu = $cpu.Name.Trim()
        logical_cpus = $cpu.NumberOfLogicalProcessors
        ram_gb = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
        os = $os.Caption
        docker_desktop = [ordered]@{ cpus = $parts[0]; memory_bytes = $parts[1]; server = $parts[2] }
    }
}

function Get-K6Count($summary, [string] $metric) {
    $m = $summary.metrics.$metric
    if ($null -eq $m) { return [decimal] 0 }
    return [decimal] $m.count
}

function Set-ProcessEnv([hashtable] $values) {
    $saved = @{}
    foreach ($name in $values.Keys) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
        [Environment]::SetEnvironmentVariable($name, $values[$name], "Process")
    }
    return $saved
}

# --- preflight ---
$credentials = Get-Credentials
Compose "start postgres, kafka and redis" @("up", "-d", "postgres", "kafka", "redis") | Out-Null
if (-not (Test-PortFree $ApiPort)) { throw "port $ApiPort is in use: stop whatever listens there first" }
if (-not (Test-PortFree $WorkerMetricsPort)) { throw "port $WorkerMetricsPort is in use" }
$newest = Get-ChildItem (Join-Path $Root "services\ledger-api\src\main") -Recurse -File | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not (Test-Path $Jar) -or (Get-Item $Jar).LastWriteTime -lt $newest.LastWriteTime) {
    Log "packaging the ledger-api jar"
    Push-Location (Join-Path $Root "services\ledger-api")
    try { Invoke-Native "package the jar" ".\mvnw.cmd" @("-q", "-DskipTests", "package") | Out-Null } finally { Pop-Location }
}
$OutputRoot = $(if ([IO.Path]::IsPathRooted($OutputDir)) { $OutputDir } else { Join-Path $Root $OutputDir })
Log "raw outputs under $OutputRoot"
$environment = Get-Environment
$weightArgs = @{}
foreach ($pair in $Weights.Split(",")) { $kv = $pair.Split("="); $weightArgs[$kv[0].Trim()] = $kv[1].Trim() }
$failures = @()

foreach ($mode in $Modes) {
    if (@("A", "B", "C-stopped", "C-paused") -notcontains $mode) { throw "unknown mode $mode" }
    for ($run = 1; $run -le $Runs; $run++) {
        $raw = Join-Path $OutputRoot ($mode + "-" + $run)
        New-Item -ItemType Directory -Force $raw | Out-Null
        Log "mode $mode run $run -> $raw"
        $isB = ($mode -eq "B")
        $apiKey = New-ApiKey
        $apiLog = Join-Path $raw "api.log"
        $workerLog = Join-Path $raw "worker.jsonl"
        $api = $null; $worker = $null; $k6 = $null
        $redisDown = $null   # "stop" or "pause" while this run has Redis down
        $k6Name = "ledger-bench-k6-$mode-$run".ToLower()
        $step = "start"
        try {
            $step = "flush Redis"
            Compose $step @("exec", "-T", "redis", "redis-cli", "FLUSHALL") | Out-Null

            $step = "start ledger-api"
            $apiArgs = @("-jar", "`"$Jar`"", "--server.address=127.0.0.1", "--server.port=$ApiPort",
                "--ledger.api-keys.$($apiKey.Hash)=bench", "--management.endpoints.web.exposure.include=health,metrics",
                "--ledger.redis.url=$RedisUrl", "--spring.datasource.url=jdbc:postgresql://${PostgresHost}:5433/ledger",
                "--ledger.outbox.relay.bootstrap-servers=$KafkaBootstrap")
            if ($isB) { $apiArgs += @("--ledger.cache.balance.ttl=1s", "--ledger.cache.balance.invalidation=false") }
            $saved = Set-ProcessEnv @{ LEDGER_API_APP_PASSWORD = $credentials["LEDGER_API_APP_PASSWORD"]; LEDGER_API_OWNER_PASSWORD = $credentials["LEDGER_API_OWNER_PASSWORD"] }
            try { $api = Start-Bench "java" $apiArgs $apiLog (Join-Path $raw "api.err") $null } finally { Set-ProcessEnv $saved | Out-Null }

            $step = "wait for ledger-api health (Flyway migrated)"
            Wait-Health $apiKey.Key $api $apiLog
            $step = "wait for the relay's topics"
            Wait-Topics
            $step = "reset the worker group to the topic's end"
            Reset-GroupToEnd

            $step = "start the worker"
            $workerEnv = @{ POSTGRES_HOST = $PostgresHost; POSTGRES_PORT = "5433"; POSTGRES_DB = "ledger";
                SETTLEMENT_WORKER_PASSWORD = $credentials["SETTLEMENT_WORKER_PASSWORD"]; KAFKA_BOOTSTRAP_SERVERS = $KafkaBootstrap;
                REDIS_URL = $RedisUrl; WORKER_CONSUMER_GROUP = $WorkerGroup; WORKER_METRICS_PORT = "$WorkerMetricsPort";
                WORKER_BALANCE_INVALIDATION = $(if ($isB) { "0" } else { "1" }); RECONCILE_INTERVAL_MS = $null; FAULT_POINT = $null;
                FAULT_PAYMENT_ID = $null }
            $saved = Set-ProcessEnv $workerEnv
            try { $worker = Start-Bench $WorkerPython @("-m", "settlement_worker") $workerLog (Join-Path $raw "worker.err") $WorkerDir } finally { Set-ProcessEnv $saved | Out-Null }
            $started = Wait-WorkerStarted $worker $workerLog

            $step = "mode guard"
            $apiOff = [bool] (Select-String -Path $apiLog -Pattern "Balance cache invalidation is OFF" -Quiet)
            $expectWorker = $(if ($isB) { "off" } else { "on" })
            if ($apiOff -ne $isB -or $started.balance_invalidation -ne $expectWorker) {
                throw "mode $mode needs the API's invalidation WARNING=$isB (saw $apiOff) and the worker's balance_invalidation=$expectWorker (saw $($started.balance_invalidation))"
            }
            Log "mode guard ok (API invalidation off=$apiOff, worker balance_invalidation=$($started.balance_invalidation))"

            $step = "wait for lag 0"
            Wait-Lag0
            $step = "record before"
            $evictedBefore = Get-EvictedKeys
            $before = Get-Counters $apiKey.Key
            (Compose "redis INFO" @("exec", "-T", "redis", "redis-cli", "INFO", "stats")).Output | Out-File -Encoding utf8 (Join-Path $raw "redis-info-before.txt")

            $step = "run k6"
            $k6Args = @("run", "--rm", "--name", $k6Name, "-v", "$(Join-Path $Root 'infra\k6'):/scripts:ro", "-v", "${raw}:/out",
                "-e", "BASE_URL=http://host.docker.internal:$ApiPort", "-e", "API_KEY=$($apiKey.Key)", "-e", "SEED=$Seed",
                "-e", "VUS=$Vus", "-e", "DURATION=${DurationSeconds}s", "-e", "CUSTOMERS_PER_VU=$CustomersPerVu",
                "-e", "DECLINE_PERCENT=$DeclinePercent", "-e", "W_DEPOSIT=$($weightArgs['deposit'])",
                "-e", "W_AUTHORIZE=$($weightArgs['authorize'])", "-e", "W_OWNED_READ=$($weightArgs['ownedRead'])",
                "-e", "W_FOREIGN_READ=$($weightArgs['foreignRead'])", $K6Image, "run", "--summary-export", "/out/k6-summary.json",
                "/scripts/cache-benchmark.js")
            $k6 = Start-Bench "docker" $k6Args (Join-Path $raw "k6.txt") (Join-Path $raw "k6.err") $null
            $outage = $null
            if ($mode -like "C-*") {
                Start-Sleep -Seconds $OutageStartSeconds
                $verb = $(if ($mode -eq "C-stopped") { "stop" } else { "pause" })
                $step = "$verb Redis"
                $outage = [ordered]@{ action = $verb; at = (Get-Date).ToUniversalTime().ToString("o") }
                Compose $step @($verb, "redis") | Out-Null
                $redisDown = $verb
                Start-Sleep -Seconds $OutageSeconds
                $back = $(if ($verb -eq "stop") { "start" } else { "unpause" })
                $step = "$back Redis"
                Compose $step @($back, "redis") | Out-Null
                $redisDown = $null
                $outage["back_at"] = (Get-Date).ToUniversalTime().ToString("o")
            }
            $step = "wait for k6"
            $k6.WaitForExit()
            $k6Exit = $k6.ExitCode

            $step = "record after"
            $after = Get-Counters $apiKey.Key
            $evictedAfter = Get-EvictedKeys
            (Compose "redis INFO" @("exec", "-T", "redis", "redis-cli", "INFO", "stats")).Output | Out-File -Encoding utf8 (Join-Path $raw "redis-info-after.txt")
            $summaryPath = Join-Path $raw "k6-summary.json"
            if (-not (Test-Path $summaryPath)) { throw "k6 exited $k6Exit with no summary; see k6.txt and k6.err" }
            $summary = Get-Content $summaryPath -Raw | ConvertFrom-Json
            $delta = [ordered]@{}
            foreach ($side in "api", "worker") { foreach ($k in $after[$side].Keys) { $delta["$side.$k"] = $after[$side][$k] - $before[$side][$k] } }
            [ordered]@{ before = $before; after = $after; delta = $delta } | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 (Join-Path $raw "counters.json")

            $step = "judge validity"
            $reasons = @()
            if ($evictedAfter -ne $evictedBefore) { $reasons += "evicted_keys changed ($evictedBefore -> $evictedAfter)" }
            if ((Get-K6Count $summary "judged_incomplete") -gt 0) { $reasons += "judged reads did not complete" }
            if ((Get-K6Count $summary "judged_reads") -le 0) { $reasons += "no judged reads (the workload never ran)" }
            # The mode really ran as configured: both services' invalidation counters over the run.
            $wSent = $delta["worker.settlement_balance_invalidations_total.sent"]; $wFailed = $delta["worker.settlement_balance_invalidations_total.failed"]
            $aSent = $delta["api.ledger.balance.cache.invalidation.sent"]; $aFailed = $delta["api.ledger.balance.cache.invalidation.failed"]
            if ($mode -eq "A") {
                if ($wFailed -gt 0) { $reasons += "worker invalidations failed with Redis up ($wFailed)" }
                if ($aFailed -gt 0) { $reasons += "API invalidations failed with Redis up ($aFailed)" }
                if ($wSent -le 0) { $reasons += "the worker sent no invalidations" }
            }
            elseif ($isB) {
                if (($wSent + $wFailed + $aSent + $aFailed) -gt 0) { $reasons += "invalidations ran in TTL-only mode (worker $wSent/$wFailed, API $aSent/$aFailed)" }
            }
            elseif ($wSent -le 0) { $reasons += "the worker sent no invalidations (outside the outage it must)" }
            $failed = @()
            # k6's exit codes: 99 = thresholds failed (here: stale_miss or stale_bypass > 0); anything else is the run's own
            # failure (107 = a script exception, e.g. setup() could not reach the API), not a finding.
            if ($k6Exit -eq 99) { $failed += "k6 thresholds failed (stale_miss or stale_bypass > 0)" }
            elseif ($k6Exit -ne 0) { $reasons += "k6 exited $k6Exit (not a threshold: see k6.txt)" }
            if ($isB -and (Get-K6Count $summary "stale_hit") -le 0) { $failed += "mode B positive control: stale_hit = 0" }
            $validity = [ordered]@{ mode = $mode; run = $run; valid = ($reasons.Count -eq 0); invalid_because = $reasons;
                passed = ($failed.Count -eq 0); failed_because = $failed; outage = $outage;
                stale_hit = (Get-K6Count $summary "stale_hit"); stale_miss = (Get-K6Count $summary "stale_miss");
                stale_bypass = (Get-K6Count $summary "stale_bypass"); judged_reads = (Get-K6Count $summary "judged_reads");
                worker_started = $started }
            $validity | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 (Join-Path $raw "validity.json")
            [ordered]@{ environment = $environment; mode = $mode; run = $run; seed = $Seed; vus = $Vus; duration_s = $DurationSeconds;
                customers_per_vu = $CustomersPerVu; weights = $Weights; decline_percent = $DeclinePercent;
                outage_start_s = $OutageStartSeconds; outage_s = $OutageSeconds } | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 (Join-Path $raw "env.json")
            Log ("mode $mode run ${run}: valid=" + $validity.valid + " passed=" + $validity.passed + " " + (($reasons + $failed) -join "; "))
            if (-not $validity.valid -or -not $validity.passed) { $failures += "$mode-$run" }
        }
        catch {
            Log ("mode $mode run $run FAILED at step '" + $step + "': " + $_.Exception.Message)
            $failures += "$mode-$run"
        }
        finally {
            # Services first, then Redis, each step on its own: nothing here may throw or skip another step.
            $stopped = @()
            $recorded = @()
            foreach ($pair in @(@("k6", $k6), @("worker", $worker), @("ledger-api", $api))) {
                try {
                    $ids = @(Stop-BenchTree $pair[0] $pair[1])
                    if ($ids.Count -gt 0) { $stopped += "$($pair[0]) (pids $($ids -join ','))"; $recorded += $ids }
                }
                catch { Log ("cleanup: stopping $($pair[0]) failed: " + $_.Exception.Message) }
            }
            try {
                $r = Invoke-Native "cleanup: remove the k6 container" "docker" @("rm", "-f", $k6Name) -AllowFailure
                if ($r.Code -eq 0 -and ($r.Output -contains $k6Name)) { $stopped += "k6 container $k6Name" }
            }
            catch { Log ("cleanup: removing the k6 container failed: " + $_.Exception.Message) }
            if ($redisDown) {
                $restore = $(if ($redisDown -eq "stop") { "start" } else { "unpause" })
                try {
                    $r = Compose "cleanup: $restore Redis" @($restore, "redis") -AllowFailure
                    $stopped += "redis $restore (exit $($r.Code))"
                }
                catch { Log ("cleanup: $restore Redis failed: " + $_.Exception.Message) }
            }
            Log ("cleanup: " + $(if ($stopped.Count) { $stopped -join "; " } else { "nothing to stop" }))
            Start-Sleep -Milliseconds 500
            $alive = @(Get-AliveIds $recorded)
            if ($alive.Count -gt 0) {
                Log ("cleanup: bench processes still alive: " + ($alive -join ","))
                if ($failures -notcontains "$mode-$run") { $failures += "$mode-$run" }
            }
            else { Log "cleanup: no bench-launched process alive" }
        }
    }
}

if ($failures.Count -gt 0) { Log ("failed or invalid: " + ($failures -join ", ")); exit 1 }
Log "every run valid and passing"
exit 0
