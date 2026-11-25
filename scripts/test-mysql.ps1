param([Parameter(Mandatory)][string]$MySQLBin, [string]$PreviousCLI, [switch]$CapacityLoad)

$ErrorActionPreference = 'Stop'
$MySQLBin = (Resolve-Path -LiteralPath $MySQLBin).Path
if ($PreviousCLI) { $PreviousCLI = (Resolve-Path -LiteralPath $PreviousCLI).Path }
$project = Split-Path $PSScriptRoot -Parent
$fixtureParent = (New-Item -ItemType Directory -Path (Join-Path $project '.local') -Force).FullName
$key = [guid]::NewGuid().ToString('N').Substring(0, 12)
$fixture = (New-Item -ItemType Directory -Path (Join-Path $fixtureParent "mysql-$key")).FullName
$binary = Join-Path $fixture 'workflow.exe'
$database = "workflow_test_$key"
$rootPassword = [guid]::NewGuid().ToString('N')
$appPassword = [guid]::NewGuid().ToString('N')
$recoveryDirectory = Join-Path $fixture 'recovery'
$recoveryRootPassword = [guid]::NewGuid().ToString('N')
$server = $null
$recoveryServer = $null
$workerAPI = $null
$console = $null
$scanners = @()
$passed = $false

function Assert-NativeExit([string]$Operation) {
    if ($LASTEXITCODE -ne 0) { throw "$Operation exited with code $LASTEXITCODE" }
}

function Get-LoopbackPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return $listener.LocalEndpoint.Port
    } finally {
        $listener.Stop()
    }
}

function Get-ConsoleCsrf([string]$Html) {
    $match = [regex]::Match($Html, 'name="csrf" value="([a-zA-Z0-9_-]{43})"')
    if (!$match.Success) { throw 'Console form is missing its session proof' }
    return $match.Groups[1].Value
}
$port = Get-LoopbackPort

$savedEnvironment = @{}
foreach ($name in @('WORKFLOW_MYSQL_DSN', 'WORKFLOW_TEST_MYSQL_DSN', 'WORKFLOW_ACTOR', 'WORKFLOW_WORKER_TOKEN', 'WORKFLOW_CAPACITY_LOAD', 'MYSQL_PWD')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$mysql = Join-Path $MySQLBin 'mysql.exe'
$mysqld = Join-Path $MySQLBin 'mysqld.exe'
$mysqldump = Join-Path $MySQLBin 'mysqldump.exe'
$mysqladmin = Join-Path $MySQLBin 'mysqladmin.exe'
$clientArgs = @('--no-defaults', '--protocol=TCP', '--host=127.0.0.1', "--port=$port", '--user=root')

function Start-FixtureDatabase([string]$Directory, [int]$Port, [string]$Database, [string]$Login, [string]$Password, [string]$AdminPassword) {
    $data = (New-Item -ItemType Directory -Path (Join-Path $Directory 'data')).FullName
    $initSQL = Join-Path $Directory 'init.sql'
    $serverArgs = @('--no-defaults', '--innodb-buffer-pool-size=32M', '--max-connections=48', '--mysqlx=OFF',
        ('--basedir="{0}"' -f (Split-Path $MySQLBin -Parent)), ('--datadir="{0}"' -f $data),
        ('--log-error="{0}/mysql.log"' -f $Directory))
    $init = Start-Process -FilePath $mysqld -ArgumentList ($serverArgs + '--initialize-insecure') -PassThru -Wait -WindowStyle Hidden
    if ($init.ExitCode -ne 0) { throw 'MySQL initialization failed' }
    @"
ALTER USER 'root'@'localhost' IDENTIFIED BY '$AdminPassword';
CREATE DATABASE $Database CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER '$Login'@'127.0.0.1' IDENTIFIED BY '$Password';
GRANT ALL PRIVILEGES ON $Database.* TO '$Login'@'127.0.0.1';
"@ | Set-Content -LiteralPath $initSQL -Encoding ascii
    Start-Process -FilePath $mysqld -ArgumentList ($serverArgs + @('--bind-address=127.0.0.1', "--port=$Port", ('--init-file="{0}"' -f $initSQL))) -PassThru -WindowStyle Hidden
}

function Wait-FixtureDatabase([Diagnostics.Process]$Process, [string[]]$Options, [string]$Directory) {
    $version = $null
    for ($i = 0; $i -lt 40; $i++) {
        if ($Process.HasExited) { throw 'MySQL exited before accepting connections' }
        $version = & $mysql @Options '--batch' '--skip-column-names' '--execute=SELECT VERSION()' 2>$null
        if ($LASTEXITCODE -eq 0 -and $version) { break }
        Start-Sleep -Milliseconds 250
    }
    if (!$version -or [string]$version -notmatch '^8\.4\.') { throw 'Could not connect to a MySQL 8.4 test server' }
    Remove-Item -LiteralPath (Join-Path $Directory 'init.sql')
    return $version
}

function Get-FixtureChecksums([string[]]$Options, [string]$Database) {
    $tables = & $mysql @Options "--database=$Database" '--batch' '--skip-column-names' '--execute=SHOW TABLES'
    Assert-NativeExit 'list fixture tables'
    if (!$tables -or ($tables | Where-Object { $_ -cnotmatch '^[a-z][a-z0-9_]*$' })) { throw 'Unexpected fixture table names' }
    $rows = & $mysql @Options "--database=$Database" '--batch' '--skip-column-names' "--execute=CHECKSUM TABLE $($tables -join ',') EXTENDED"
    Assert-NativeExit 'calculate fixture table checksums'
    if ($rows.Count -ne $tables.Count -or ($rows | Where-Object { $_ -notmatch '\t\d+$' })) { throw 'Missing fixture table checksum' }
    $rows | ForEach-Object { ($_ -split '\.', 2)[1] }
}

function Read-RecoveryEvidence([string[]]$Arguments) {
    $response = & $binary @Arguments
    Assert-NativeExit "read recovery evidence for $($Arguments[0])"
    if ($Arguments[0] -eq 'run-export') {
        $report = $response -join "`n" | ConvertFrom-Json -AsHashtable -Depth 100
        $report.Remove('generated_at')
        return $report | ConvertTo-Json -Depth 100 -Compress
    }
    return $response -join "`n"
}

function Stop-FixtureDatabase([Diagnostics.Process]$Process, [string[]]$Options, [string]$Password) {
    if (!$Process -or $Process.HasExited) { return }
    $env:MYSQL_PWD = $Password
    try {
        & $mysqladmin @Options shutdown 2>$null | Out-Null
    } finally {
        if (!$Process.WaitForExit(5000)) {
            Stop-Process -Id $Process.Id
            if (!$Process.WaitForExit(5000)) { throw 'Fixture MySQL failed to stop' }
        }
    }
}

Push-Location -LiteralPath $project
try {
    $server = Start-FixtureDatabase $fixture $port $database workflow_test $appPassword $rootPassword
    $env:MYSQL_PWD = $rootPassword
    $version = Wait-FixtureDatabase $server $clientArgs $fixture
    Write-Output "Isolated MySQL $version"

    $env:WORKFLOW_TEST_MYSQL_DSN = "workflow_test:${appPassword}@tcp(127.0.0.1:$port)/$database"
    $env:WORKFLOW_MYSQL_DSN = $env:WORKFLOW_TEST_MYSQL_DSN
    $env:WORKFLOW_ACTOR = 'fixture-operator'
    if ($CapacityLoad) { $env:WORKFLOW_CAPACITY_LOAD = '1' }
    if ($PreviousCLI) {
        $previousMigration = & $PreviousCLI migrate | ConvertFrom-Json
        Assert-NativeExit 'previous CLI migrate'
        if ($previousMigration.schema_version -ne 2) { throw 'Previous CLI must use schema 2' }
        & $PreviousCLI workspace-create upgrade | Out-Null
        Assert-NativeExit 'previous CLI create workspace'
        $previousGraph = & $PreviousCLI publish upgrade 0 examples/diamond.json | ConvertFrom-Json
        Assert-NativeExit 'previous CLI publish'
        $previousSchedule = & $PreviousCLI schedule-put upgrade dispatch-report 0 examples/dispatch-schedule.json | ConvertFrom-Json
        Assert-NativeExit 'previous CLI save schedule'
        $previousScan = & $PreviousCLI scheduler --once --at '2026-11-01T01:00:00+08:00' | ConvertFrom-Json
        Assert-NativeExit 'previous CLI create scheduled run'
        $previousRun = @($previousScan.outcomes | Where-Object schedule_id -eq $previousSchedule.id | ForEach-Object run_ids)
        if ($previousRun.Count -ne 1) { throw 'Expected one pre-upgrade scheduled run' }
    }
    go test -count=1 -v ./...
    Assert-NativeExit 'Go tests'
    if ($env:WORKFLOW_CAPACITY_LOAD -eq '1') {
        $server.Refresh()
        Write-Output ('MySQL fixture peak working set: {0:N1} MiB' -f ($server.PeakWorkingSet64 / 1MB))
    }
    go vet ./...
    Assert-NativeExit 'Go vet'
    go build -o $binary ./cmd/workflow
    Assert-NativeExit 'build workflow CLI'

    $migration = & $binary migrate | ConvertFrom-Json
    Assert-NativeExit 'CLI migrate'
    if ($migration.schema_version -lt 3) { throw 'Unexpected fixture schema version' }
    if ($PreviousCLI) {
        $oldRun = & $binary run upgrade $previousRun[0] | ConvertFrom-Json
        Assert-NativeExit 'read scheduled run after schema upgrade'
        $oldPreview = & $binary backfill-preview upgrade dispatch-report hourly-dispatch 1 '2026-11-01T01:00:00+08:00' '2026-11-01T03:00:00+08:00' | ConvertFrom-Json
        Assert-NativeExit 'preview pre-upgrade occurrence'
        if ($oldRun.run.id -ne $previousRun[0] -or $oldRun.run.trigger_kind -ne 'scheduled' -or
            $oldRun.version.graph.digest -ne $previousGraph.graph.digest -or $oldPreview.items.Count -ne 3 -or
            $oldPreview.items[0].state -ne 'already_registered' -or $oldPreview.items[0].baseline_run_id -ne $previousRun[0]) {
            throw 'Schema upgrade changed the existing scheduled occurrence'
        }
        Write-Output 'Schema 2 data survived the upgrade and was found as the existing backfill baseline.'
    }
    & $binary workspace-create demo | Out-Null
    Assert-NativeExit 'CLI create workspace'
    $graph = & $binary publish demo 0 examples/diamond.json | ConvertFrom-Json
    Assert-NativeExit 'CLI publish'
    $created = & $binary trigger demo dispatch-report demo-001 examples/diamond-inputs.json | ConvertFrom-Json
    Assert-NativeExit 'CLI create run'
    $retry = & $binary trigger demo dispatch-report demo-001 examples/diamond-inputs.json | ConvertFrom-Json
    Assert-NativeExit 'CLI replay trigger'
    $detail = & $binary run demo $created.run.id | ConvertFrom-Json
    Assert-NativeExit 'CLI read run'
    if ($graph.revision -ne 1 -or $created.replayed -or !$retry.replayed -or $created.run.id -ne $retry.run.id -or
        $detail.version.graph.digest -ne $graph.graph.digest -or $detail.nodes.Count -ne 5) {
        throw 'Unexpected CLI response'
    }
    Write-Output 'CLI migration, workspace creation, publication, trigger deduplication, and run readback passed.'

    $sharedPool = & $binary pool-put report-executors 0 2 | ConvertFrom-Json
    Assert-NativeExit 'configure shared pool'
    $workspaceQuota = & $binary workspace-quota-put demo 0 3 | ConvertFrom-Json
    Assert-NativeExit 'configure workspace quota'
    $queueQuota = & $binary queue-quota-put demo reports 0 2 report-executors | ConvertFrom-Json
    Assert-NativeExit 'bind report queue to shared pool'
    $readPool = & $binary pool-get report-executors | ConvertFrom-Json
    Assert-NativeExit 'read shared pool'
    $readWorkspaceQuota = & $binary workspace-quota-get demo | ConvertFrom-Json
    Assert-NativeExit 'read workspace quota'
    $readQueueQuota = & $binary queue-quota-get demo reports | ConvertFrom-Json
    Assert-NativeExit 'read queue quota'
    if ($sharedPool.generation -ne 1 -or $readPool.max_active -ne 2 -or $workspaceQuota.max_active -ne 3 -or
        $readWorkspaceQuota.generation -ne 1 -or $queueQuota.generation -ne 1 -or $readQueueQuota.pool -ne 'report-executors') {
        throw 'Unexpected capacity configuration or readback'
    }

    $workerA = & $binary worker-register demo stage-a 1 reports:extract reports:aggregate | ConvertFrom-Json
    Assert-NativeExit 'register stage-a worker'
    $workerB = & $binary worker-register demo stage-b 1 reports:inspect reports:report | ConvertFrom-Json
    Assert-NativeExit 'register stage-b worker'
    $apiPort = Get-LoopbackPort
    $apiOrigin = "http://127.0.0.1:$apiPort"
    $workerAPI = Start-Process -FilePath $binary -ArgumentList @('worker-api', '--listen', "127.0.0.1:$apiPort") -WindowStyle Hidden -PassThru `
        -RedirectStandardError (Join-Path $fixture 'worker-api.log')
    $healthy = $false
    for ($i = 0; $i -lt 40; $i++) {
        if ($workerAPI.HasExited) { throw 'Worker API exited before accepting connections' }
        try {
            $health = Invoke-WebRequest -Uri "$apiOrigin/healthz" -TimeoutSec 1
            if ($health.StatusCode -eq 204) { $healthy = $true; break }
        } catch {
            Start-Sleep -Milliseconds 100
        }
    }
    if (!$healthy) { throw 'Worker API failed its health check' }
    $fixtureDSN = $env:WORKFLOW_MYSQL_DSN
    $env:WORKFLOW_MYSQL_DSN = $null
    $env:WORKFLOW_TEST_MYSQL_DSN = $null
    $env:MYSQL_PWD = $null
    $env:WORKFLOW_WORKER_TOKEN = $workerB.token
    $idle = & $binary worker --once --endpoint $apiOrigin | ConvertFrom-Json
    Assert-NativeExit 'worker respects its own queue and tasks'
    if ($null -ne $idle) { throw 'Stage-b worker claimed the extract node' }
    foreach ($stage in @('extract', 'aggregate', 'inspect', 'report')) {
        $env:WORKFLOW_WORKER_TOKEN = if ($stage -in @('extract', 'aggregate')) { $workerA.token } else { $workerB.token }
        $result = & $binary worker --once --endpoint $apiOrigin | ConvertFrom-Json
        Assert-NativeExit "execute $stage through the Worker API"
        if ($result.run_id -ne $created.run.id -or $result.node -ne $stage -or $result.state -ne 'succeeded' -or $result.epoch -ne 1) {
            throw "Unexpected Worker receipt for $stage"
        }
    }
    $env:WORKFLOW_WORKER_TOKEN = $null
    $env:WORKFLOW_MYSQL_DSN = $fixtureDSN
    $env:WORKFLOW_TEST_MYSQL_DSN = $fixtureDSN
    $env:MYSQL_PWD = $rootPassword
    $completedRun = & $binary run demo $created.run.id | ConvertFrom-Json
    Assert-NativeExit 'read worker execution results'
    $report = $completedRun.nodes | Where-Object key -eq report
    $cleanup = $completedRun.nodes | Where-Object key -eq cleanup
    if ($completedRun.run.state -ne 'succeeded' -or $report.outputs.summary -ne '12 dispatch records (passed)' -or
        $report.attempts[0].worker -ne 'stage-b' -or $cleanup.state -ne 'skipped') {
        throw 'Unexpected worker execution result'
    }
    $emptyPool = & $binary pool-get report-executors | ConvertFrom-Json
    Assert-NativeExit 'read released pool'
    if ($report.attempts[0].pool -ne 'report-executors' -or $emptyPool.active -ne 0) {
        throw 'Pool slot was not released or pool history was not recorded'
    }
    Write-Output 'Token-only workers executed four diamond nodes through HTTP. The result and skipped branch were read from MySQL.'

    $badInput = Join-Path $fixture 'invalid-dispatch-rows.json'
    [IO.File]::WriteAllText($badInput, '{"rows":1001,"source":"fixture"}')
    $failedRun = & $binary trigger demo dispatch-report dispatch-rerun-demo $badInput | ConvertFrom-Json
    Assert-NativeExit 'create the rerun source'
    $env:WORKFLOW_WORKER_TOKEN = $workerA.token
    $failure = & $binary worker --once --endpoint $apiOrigin | ConvertFrom-Json
    Assert-NativeExit 'produce the original task failure'
    if ($failure.run_id -ne $failedRun.run.id -or $failure.node -ne 'extract' -or $failure.state -ne 'failed') {
        throw 'Unexpected rerun fixture failure'
    }
    $rerunPreview = & $binary run-rerun-preview demo $failedRun.run.id extract | ConvertFrom-Json
    Assert-NativeExit 'preview a partial rerun'
    $rerun = & $binary run-rerun demo $failedRun.run.id inspect-failure extract | ConvertFrom-Json
    Assert-NativeExit 'register a partial rerun'
    $rerunReplay = & $binary run-rerun demo $failedRun.run.id inspect-failure extract | ConvertFrom-Json
    Assert-NativeExit 'replay the partial rerun request'
    $newFailure = & $binary worker --once --endpoint $apiOrigin | ConvertFrom-Json
    Assert-NativeExit 'execute the derived run'
    $env:WORKFLOW_WORKER_TOKEN = $null
    $sourceReport = & $binary run-export demo $failedRun.run.id | ConvertFrom-Json
    Assert-NativeExit 'export the source run'
    $derivedReport = & $binary run-export demo $rerun.run_id | ConvertFrom-Json
    Assert-NativeExit 'export the derived run'
    if ($rerunPreview.revision -ne 1 -or $rerunPreview.selected[0] -ne 'extract' -or !$rerunReplay.replayed -or
        $rerunReplay.run_id -ne $rerun.run_id -or $newFailure.run_id -ne $rerun.run_id -or $newFailure.state -ne 'failed' -or
        $sourceReport.format -ne 'workflow-runtime/run-report-v1' -or $sourceReport.detail.run.id -eq $derivedReport.detail.run.id -or
        $sourceReport.detail.run.id -ne $derivedReport.detail.rerun.plan.parent_run_id -or
        $sourceReport.detail.input_digest -ne $derivedReport.detail.input_digest -or
        $sourceReport.detail.recent_reruns[0].run_id -ne $derivedReport.detail.run.id -or $derivedReport.detail.run.state -ne 'failed') {
        throw 'Unexpected partial rerun history'
    }
    Write-Output 'CLI previewed, registered, replayed, and exported a partial rerun with separate source and derived reports.'

    $operator = & $binary console-grant demo console-operator operator | ConvertFrom-Json
    Assert-NativeExit 'create a console operator'
    $author = & $binary console-grant demo console-author author | ConvertFrom-Json
    Assert-NativeExit 'create a console author'
    & $binary workspace-create isolated | Out-Null
    Assert-NativeExit 'create a separate console workspace'
    $viewer = & $binary console-grant isolated console-viewer viewer | ConvertFrom-Json
    Assert-NativeExit 'create a console viewer in the separate workspace'
    $consolePort = Get-LoopbackPort
    $consoleOrigin = "http://127.0.0.1:$consolePort"
    $console = Start-Process -FilePath $binary -ArgumentList @('console', '--listen', "127.0.0.1:$consolePort") -WindowStyle Hidden -PassThru `
        -RedirectStandardError (Join-Path $fixture 'console.log')
    $healthy = $false
    for ($i = 0; $i -lt 40; $i++) {
        if ($console.HasExited) { throw 'Console exited before accepting connections' }
        try {
            $health = Invoke-WebRequest -Uri "$consoleOrigin/healthz" -TimeoutSec 1
            if ($health.StatusCode -eq 204) { $healthy = $true; break }
        } catch {
            Start-Sleep -Milliseconds 100
        }
    }
    if (!$healthy) { throw 'Console failed its health check' }
    $publicView = Invoke-WebRequest -Uri "$consoleOrigin/"
    if ($publicView.Content -notmatch 'name="credential"') { throw 'Console did not require authentication' }
    $operatorView = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/login" -SessionVariable ops `
        -Body @{credential = $operator.token} -ContentType 'application/x-www-form-urlencoded'
    if ($operatorView.Content -notmatch 'console-operator' -or $operatorView.Content -notmatch 'dispatch-report') { throw 'Console did not show the operator workspace' }
    $csrf = Get-ConsoleCsrf $operatorView.Content
    $unused = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/login" -SessionVariable readOnly `
        -Body @{credential = $viewer.token} -ContentType 'application/x-www-form-urlencoded'
    $foreignReport = Invoke-WebRequest -Uri "$consoleOrigin/runs/$($rerun.run_id)/report.json" -WebSession $readOnly -SkipHttpErrorCheck
    if ($foreignReport.StatusCode -ne 404) { throw 'A viewer read another workspace report' }
    $unused = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/login" -SessionVariable writer `
        -Body @{credential = $author.token} -ContentType 'application/x-www-form-urlencoded'
    $authorCsrf = Get-ConsoleCsrf $unused.Content
    $draft = (Get-Content -Raw -LiteralPath (Join-Path $project 'examples/diamond.json')).Replace('"dispatch-report"', '"console-dispatch"')
    $validated = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/editor" -WebSession $writer `
        -Body @{csrf = $authorCsrf; revision = '0'; source = $draft; command = 'validate'} -ContentType 'application/x-www-form-urlencoded'
    if ($validated.Content -notmatch '<svg class="dag"' -or $validated.Content -notmatch 'class="graph-node') { throw 'Author did not get the validated graph' }
    $forbidden = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/editor" -WebSession $writer -SkipHttpErrorCheck `
        -Body @{csrf = $authorCsrf; revision = '0'; source = $draft; command = 'publish'} -ContentType 'application/x-www-form-urlencoded'
    $untrusted = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/editor" -WebSession $ops -SkipHttpErrorCheck `
        -Body @{revision = '0'; source = $draft; command = 'publish'} -ContentType 'application/x-www-form-urlencoded'
    if ($forbidden.StatusCode -ne 403 -or $untrusted.StatusCode -ne 403) { throw 'Console accepted a forbidden publication' }
    $published = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/editor" -WebSession $ops `
        -Body @{csrf = $csrf; revision = '0'; source = $draft; command = 'publish'} -ContentType 'application/x-www-form-urlencoded'
    if ($published.Content -notmatch '<h1>console-dispatch</h1>' -or $published.Content -notmatch 'name="logical_at"') {
        throw 'Console publication did not open the workflow page'
    }
    $changedDraft = $draft.Replace('"timeout_seconds": 30', '"timeout_seconds": 35')
    $conflict = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/editor" -WebSession $ops -SkipHttpErrorCheck `
        -Body @{csrf = $csrf; revision = '0'; source = $changedDraft; command = 'publish'} -ContentType 'application/x-www-form-urlencoded'
    if ($conflict.StatusCode -ne 409 -or $conflict.Content -notmatch 'extract') { throw 'Console publication failed to show a stale revision' }
    $registered = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/workflows/console-dispatch/trigger" -WebSession $ops `
        -Body @{csrf = $csrf; revision = '1'; key = 'console-demo'; kind = 'manual'; inputs = '{"rows":12,"source":"console"}'} `
        -ContentType 'application/x-www-form-urlencoded'
    $registeredID = [regex]::Match($registered.Content, '/runs/(\d+)/report\.json').Groups[1].Value
    if (!$registeredID -or $registered.Content -notmatch 'id="node-extract"') { throw 'Console trigger did not open the registered run' }
    $filtered = Invoke-WebRequest -Uri "$consoleOrigin/?state=queued&dag=console-dispatch" -WebSession $ops
    if ($filtered.Content -notmatch "/runs/$registeredID" -or $filtered.Content -match 'dispatch-rerun-demo') { throw 'Console run filtering returned an unexpected run' }
    $canceled = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/runs/$registeredID/cancel" -WebSession $ops `
        -Body @{csrf = $csrf; key = 'console-stop'; grace = '0'; reason = 'test-stop'} -ContentType 'application/x-www-form-urlencoded'
    if ($canceled.Content -notmatch 'test-stop') { throw 'Console did not read back the cancellation' }
    $browserPreview = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/runs/$($failedRun.run.id)/rerun-preview" -WebSession $ops `
        -Body @{csrf = $csrf; key = 'console-rerun'; nodes = 'extract'} -ContentType 'application/x-www-form-urlencoded'
    if ($browserPreview.Content -notmatch 'name="key" value="console-rerun"' -or $browserPreview.Content -notmatch 'name="nodes" value="extract"') { throw 'Console rerun preview omitted the selected node' }
    $browserRerun = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/runs/$($failedRun.run.id)/rerun" -WebSession $ops `
        -Body @{csrf = $csrf; key = 'console-rerun'; nodes = 'extract'} -ContentType 'application/x-www-form-urlencoded'
    $browserChildID = [regex]::Match($browserRerun.Content, '/runs/(\d+)/compare').Groups[1].Value
    if (!$browserChildID -or $browserChildID -eq [string]$rerun.run_id) { throw 'Console did not create a separate child run' }
    $comparison = Invoke-WebRequest -Uri "$consoleOrigin/runs/$browserChildID/compare" -WebSession $ops
    $browserReport = Invoke-RestMethod -Uri "$consoleOrigin/runs/$browserChildID/report.json" -WebSession $ops
    if ($comparison.Content -notmatch "/runs/$($failedRun.run.id)/report.json" -or $browserReport.detail.rerun.actor -ne 'console-operator' -or
        $browserReport.detail.rerun.plan.parent_run_id -ne $failedRun.run.id) { throw 'Console reports lost source or actor information' }

    $plansURL = "$consoleOrigin/workflows/console-dispatch/schedules"
    $planURL = "$plansURL/console-hourly"
    $planForm = @{csrf = $csrf; generation = '0'; key = 'console-hourly'; cron = '0 * * * *'; zone = 'Asia/Shanghai';
        misfire = 'catch_up'; grace = '60'; limit = '1'; revision = '1'; start_after = '2025-11-01T00:00:00+08:00';
        inputs = '{"rows":2,"source":"console"}'; enabled = '1'}
    $savedPlan = Invoke-WebRequest -Method Post -Uri $plansURL -WebSession $ops -Body $planForm -ContentType 'application/x-www-form-urlencoded'
    if ($savedPlan.Content -notmatch 'name="generation" value="1"' -or $savedPlan.Content -notmatch 'Asia/Shanghai') { throw 'Console did not show the new schedule' }
    $plan = & $binary schedule-get demo console-dispatch console-hourly | ConvertFrom-Json
    Assert-NativeExit 'read the console schedule through the CLI'
    if (!$plan.enabled -or $plan.revision -ne 1 -or $plan.max_active_runs -ne 1) { throw 'Console schedule was not persisted with its submitted values' }
    $planForm.generation = '1'
    $planForm.enabled = '0'
    $paused = Invoke-WebRequest -Method Post -Uri $planURL -WebSession $ops -Body $planForm -ContentType 'application/x-www-form-urlencoded'
    if ($paused.Content -notmatch 'name="generation" value="2"') { throw 'Pause did not advance the schedule generation' }
    $stalePlan = Invoke-WebRequest -Method Post -Uri $planURL -WebSession $ops -Body $planForm -ContentType 'application/x-www-form-urlencoded' -SkipHttpErrorCheck
    $readPlan = & $binary schedule-get demo console-dispatch console-hourly | ConvertFrom-Json
    Assert-NativeExit 'read the paused console schedule'
    $foreignPlan = Invoke-WebRequest -Uri $planURL -WebSession $readOnly -SkipHttpErrorCheck
    if ($stalePlan.StatusCode -ne 409 -or $readPlan.enabled -or $readPlan.next_at -ne $plan.next_at -or $foreignPlan.StatusCode -ne 404) {
        throw 'Schedule pause, generation fencing, cursor preservation, or workspace isolation failed'
    }

    $preview = Invoke-WebRequest -Method Post -Uri "$planURL/backfill" -WebSession $ops -ContentType 'application/x-www-form-urlencoded' `
        -Body @{csrf = $csrf; revision = '1'; from = '2025-11-01T01:00:00+08:00'; through = '2025-11-01T07:00:00+08:00'}
    $batchID = [regex]::Match($preview.Content, 'data-batch-id="(\d+)"').Groups[1].Value
    if (!$batchID -or [regex]::Matches($preview.Content, 'name="ordinals"').Count -ne 7) { throw 'Console preview did not show exactly seven selectable times' }
    $selection = "csrf=$csrf&ordinals=1&ordinals=3&ordinals=6"
    $submitted = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/backfills/$batchID/confirm" -WebSession $ops `
        -Body $selection -ContentType 'application/x-www-form-urlencoded'
    if ([regex]::Matches($submitted.Content, 'data-state="created"').Count -ne 1 -or
        [regex]::Matches($submitted.Content, 'data-state="pending"').Count -ne 2 -or
        [regex]::Matches($submitted.Content, 'data-state="excluded"').Count -ne 4) { throw 'Console backfill did not preserve the capacity-blocked occurrences' }
    $selectionConflict = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/backfills/$batchID/confirm" -WebSession $ops `
        -Body "csrf=$csrf&ordinals=2" -ContentType 'application/x-www-form-urlencoded' -SkipHttpErrorCheck
    $foreignBatch = Invoke-WebRequest -Uri "$consoleOrigin/backfills/$batchID" -WebSession $readOnly -SkipHttpErrorCheck
    if ($selectionConflict.StatusCode -ne 409 -or $foreignBatch.StatusCode -ne 404) { throw 'Backfill selection or workspace boundary was bypassed' }
    $firstReceipt = & $binary backfill-get demo $batchID | ConvertFrom-Json
    Assert-NativeExit 'read the first backfill receipt'
    $ids = @($firstReceipt.items[0].run_id)
    foreach ($index in @(2, 5)) {
        $unused = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/runs/$($ids[-1])/cancel" -WebSession $ops `
            -Body @{csrf = $csrf; key = "free-backfill-slot-$index"; grace = '0'; reason = 'free the fixture schedule slot'} -ContentType 'application/x-www-form-urlencoded'
        $unused = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/backfills/$batchID/resume" -WebSession $ops `
            -Body @{csrf = $csrf} -ContentType 'application/x-www-form-urlencoded'
        $receipt = & $binary backfill-get demo $batchID | ConvertFrom-Json
        Assert-NativeExit 'read a resumed backfill receipt'
        if (!$receipt.items[$index].run_id) { throw 'Resumed backfill did not allocate the next run' }
        $ids += $receipt.items[$index].run_id
    }
    $replayedBatch = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/backfills/$batchID/confirm" -WebSession $ops `
        -Body $selection -ContentType 'application/x-www-form-urlencoded'
    $batchIndex = Invoke-WebRequest -Uri "$consoleOrigin/backfills" -WebSession $ops
    $finalBatch = & $binary backfill-get demo $batchID | ConvertFrom-Json
    Assert-NativeExit 'read the final backfill receipts'
    $finalIDs = @($finalBatch.items | Where-Object state -eq created | ForEach-Object run_id)
    if ($finalBatch.state -ne 'completed' -or ($ids -join ',') -ne ($finalIDs -join ',') -or $finalIDs.Count -ne 3 -or
        $batchIndex.Content -notmatch "href=`"/backfills/$batchID`"" -or $replayedBatch.Content -notmatch 'data-state="created"') {
        throw 'Console did not recover and replay the same three backfill receipts'
    }

    $capacityHome = Invoke-WebRequest -Uri "$consoleOrigin/capacity?queue=reports" -WebSession $ops
    if ($capacityHome.Content -notmatch 'report-executors' -or $capacityHome.Content -notmatch 'action="/capacity/queue"') {
        throw 'Console did not display the queue capacity and its existing pool'
    }
    $workspaceLimit = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/capacity/workspace" -WebSession $ops `
        -Body @{csrf = $csrf; generation = '1'; maximum = '4'} -ContentType 'application/x-www-form-urlencoded'
    $queueLimit = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/capacity/queue" -WebSession $ops `
        -Body @{csrf = $csrf; generation = '1'; maximum = '1'; queue = 'reports'; pool = 'unassigned-pool'} -ContentType 'application/x-www-form-urlencoded'
    $staleQueue = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/capacity/queue" -WebSession $ops -SkipHttpErrorCheck `
        -Body @{csrf = $csrf; generation = '1'; maximum = '0'; queue = 'reports'} -ContentType 'application/x-www-form-urlencoded'
    $otherCapacity = Invoke-WebRequest -Uri "$consoleOrigin/capacity?queue=reports" -WebSession $readOnly
    $otherCsrf = Get-ConsoleCsrf $otherCapacity.Content
    $deniedQuota = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/capacity/queue" -WebSession $readOnly -SkipHttpErrorCheck `
        -Body @{csrf = $otherCsrf; generation = '0'; maximum = '0'; queue = 'reports'} -ContentType 'application/x-www-form-urlencoded'
    $readWorkspace = & $binary workspace-quota-get demo | ConvertFrom-Json
    Assert-NativeExit 'read the console workspace quota'
    $readQueue = & $binary queue-quota-get demo reports | ConvertFrom-Json
    Assert-NativeExit 'read the console queue quota'
    if ($workspaceLimit.StatusCode -ne 200 -or $queueLimit.StatusCode -ne 200 -or $staleQueue.StatusCode -ne 409 -or
        $deniedQuota.StatusCode -ne 403 -or $otherCapacity.Content -match 'report-executors' -or $readWorkspace.max_active -ne 4 -or
        $readQueue.max_active -ne 1 -or $readQueue.generation -ne 2 -or $readQueue.pool -ne 'report-executors') {
        throw 'Console quota generation, saved values, or role boundary failed'
    }
    Write-Output 'Console schedule pause preserved its cursor. Backfill froze three of seven times and resumed as slots reopened. Console quota changes preserved the shared-pool assignment.'

    & $binary console-revoke demo console-operator | Out-Null
    Assert-NativeExit 'revoke the console operator'
    $closed = Invoke-WebRequest -Uri "$consoleOrigin/runs/$browserChildID" -WebSession $ops
    if ($closed.Content -notmatch 'name="credential"') { throw 'Revocation left behind an active console session' }
    Write-Output 'Console authentication, workspace isolation, author permission, DAG publication and conflict, trigger, cancel, rerun and report all passed over HTTP.'

    $externalGraph = Join-Path $fixture 'external-action.json'
    [IO.File]::WriteAllText($externalGraph, @'
{"key":"external-action","nodes":[
  {"key":"ship","task":"ship","queue":"shipping","effect":"external","timeout_seconds":60,"retry":{"max_attempts":1}},
  {"key":"notify","task":"notify","queue":"shipping","effect":"none","timeout_seconds":30,"retry":{"max_attempts":1},
   "depends_on":[{"node":"ship","when":"finished"}]}]}
'@)
    $emptyInputs = Join-Path $fixture 'empty-inputs.json'
    [IO.File]::WriteAllText($emptyInputs, '{}')
    & $binary publish demo 0 $externalGraph | Out-Null
    Assert-NativeExit 'publish external action'
    $shipment = & $binary trigger demo external-action external-001 $emptyInputs | ConvertFrom-Json
    Assert-NativeExit 'trigger external action'
    $courier = & $binary worker-register demo courier 1 shipping:ship shipping:notify | ConvertFrom-Json
    Assert-NativeExit 'register external worker'
    $courierHeaders = @{ Authorization = "Bearer $($courier.token)" }
    $externalClaim = Invoke-RestMethod -Method Post -Uri "$apiOrigin/v1/worker/claim" -ContentType 'application/json' -Headers $courierHeaders `
        -Body '{"tasks":["ship"],"lease_seconds":30}'
    if ($externalClaim.task.run_id -ne $shipment.run.id) { throw 'Wrong external claim' }
    $taskLease = @{
        run_id = $externalClaim.task.run_id; node = $externalClaim.task.node
        epoch = $externalClaim.task.epoch; operation_key = $externalClaim.task.operation_key
    }
    $stop = & $binary run-cancel demo $shipment.run.id shipping-incident 'Wrong destination' | ConvertFrom-Json
    Assert-NativeExit 'request cancellation'
    $renewBody = @{ lease = $taskLease; lease_seconds = 30 } | ConvertTo-Json -Compress
    $renewal = Invoke-RestMethod -Method Post -Uri "$apiOrigin/v1/worker/renew" -ContentType 'application/json' -Headers $courierHeaders -Body $renewBody
    if (!$renewal.cancel_deadline -or $renewal.cancel_deadline -ne $stop.deadline) { throw 'Worker API did not signal cancellation' }
    $stopReceipt = Invoke-RestMethod -Method Post -Uri "$apiOrigin/v1/worker/cancel-ack" -ContentType 'application/json' -Headers $courierHeaders `
        -Body ($taskLease | ConvertTo-Json -Compress)
    if ($stopReceipt.state -ne 'action_required') { throw 'External action did not await reconciliation' }
    $externalResult = Join-Path $fixture 'reconciliation.json'
    [IO.File]::WriteAllText($externalResult, (@{
        request_key = 'carrier-9020'; epoch = $taskLease.epoch; evidence = 'Carrier receipt 9020'; state = 'succeeded'
    } | ConvertTo-Json))
    $resolution = & $binary run-resolve demo $shipment.run.id ship $externalResult | ConvertFrom-Json
    Assert-NativeExit 'reconcile external action'
    $closed = & $binary run demo $shipment.run.id | ConvertFrom-Json
    Assert-NativeExit 'read cancellation evidence'
    if ($resolution.operation_key -ne $taskLease.operation_key -or $closed.run.state -ne 'canceled' -or !$closed.cancellation.finished_at -or
        ($closed.nodes | Where-Object key -eq ship).external_resolution.evidence -ne 'Carrier receipt 9020' -or
        ($closed.nodes | Where-Object key -eq notify).state -ne 'canceled') {
        throw 'Cancellation or reconciliation evidence was lost'
    }
    Write-Output 'CLI canceled a live external action, obtained the HTTP worker acknowledgement, and recorded reconciliation without releasing downstream.'

    $schedule = & $binary schedule-put demo dispatch-report 0 examples/dispatch-schedule.json | ConvertFrom-Json
    Assert-NativeExit 'CLI save schedule'
    $scanArguments = @('scheduler', '--once', '--at', '2026-11-01T03:00:00+08:00')
    foreach ($suffix in @('a', 'b')) {
        $env:WORKFLOW_ACTOR = "scanner-$suffix"
        $scanners += Start-Process -FilePath $binary -ArgumentList $scanArguments -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $fixture "scan-$suffix.json") `
            -RedirectStandardError (Join-Path $fixture "scan-$suffix.log")
    }
    foreach ($scanner in $scanners) {
        if (!$scanner.WaitForExit(15000) -or $scanner.ExitCode -ne 0) { throw 'A scheduler process failed' }
    }
    $scheduledIDs = @(
        foreach ($suffix in @('a', 'b')) {
            $scan = Get-Content -Raw -LiteralPath (Join-Path $fixture "scan-$suffix.json") | ConvertFrom-Json
            foreach ($outcome in $scan.outcomes) {
                if ($outcome.schedule_id -eq $schedule.id) { $outcome.run_ids }
            }
        }
    )
    if ($scheduledIDs.Count -ne 3 -or @($scheduledIDs | Sort-Object -Unique).Count -ne 3) { throw 'Expected three distinct scheduled runs' }
    $current = & $binary schedule-get demo dispatch-report hourly-dispatch | ConvertFrom-Json
    Assert-NativeExit 'CLI read schedule'
    if ($schedule.generation -ne 1 -or $current.id -ne $schedule.id -or $current.next_at -eq $schedule.next_at) {
        throw 'The schedule cursor did not advance'
    }
    Write-Output 'Two scheduler CLI processes created exactly three distinct catch-up runs.'

    $backfillScheduleFile = Join-Path $fixture 'backfill-schedule.json'
    $backfillSchedule = @{
        key = 'selective-dispatch'; cron = '0 * * * *'; timezone = 'Asia/Shanghai'; misfire = 'catch_up'
        max_active_runs = 2; enabled = $false; start_after = '2025-03-01T00:00:00+08:00'
        inputs = @{ rows = 12; source = 'synthetic-harbor-shifts' }
    } | ConvertTo-Json -Depth 6
    [IO.File]::WriteAllText($backfillScheduleFile, $backfillSchedule)
    & $binary schedule-put demo dispatch-report 0 $backfillScheduleFile | Out-Null
    Assert-NativeExit 'CLI save backfill schedule'
    $nextDefinition = Get-Content -Raw -LiteralPath (Join-Path $project 'examples/diamond.json') | ConvertFrom-Json
    $nextDefinition.inputs | Add-Member -NotePropertyName batch -NotePropertyValue integer
    $nextDefinitionFile = Join-Path $fixture 'definition-v2.json'
    [IO.File]::WriteAllText($nextDefinitionFile, ($nextDefinition | ConvertTo-Json -Depth 30))
    $nextGraph = & $binary publish demo 1 $nextDefinitionFile | ConvertFrom-Json
    Assert-NativeExit 'CLI publish second revision'
    $preview = & $binary backfill-preview demo dispatch-report selective-dispatch 1 '2025-03-01T01:00:00+08:00' '2025-03-01T07:00:00+08:00' | ConvertFrom-Json
    Assert-NativeExit 'CLI preview seven logical times'
    if ($nextGraph.revision -ne 2 -or $preview.items.Count -ne 7 -or $preview.revision -ne 1) { throw 'Wrong backfill preview' }
    $partial = & $binary backfill-confirm demo $preview.id '1,3,6' | ConvertFrom-Json
    Assert-NativeExit 'CLI confirm selected times'
    $partialIDs = @($partial.items | Where-Object state -eq created | ForEach-Object run_id)
    $pending = @($partial.items | Where-Object state -eq pending)
    if ($partial.state -ne 'confirmed' -or $partialIDs.Count -ne 2 -or $pending.Count -ne 1 -or $pending[0].ordinal -ne 6) {
        throw 'Expected two committed items and one pending item'
    }
    $finishedFixtureRun = [long]$partialIDs[0]
    & $mysql @clientArgs "--database=$database" "--execute=UPDATE runs SET state='succeeded' WHERE id=$finishedFixtureRun"
    Assert-NativeExit 'release one fixture schedule slot'
    $receipt = & $binary backfill-resume demo $preview.id | ConvertFrom-Json
    Assert-NativeExit 'CLI resume remaining item in a new process'
    $again = & $binary backfill-confirm demo $preview.id '6,3,1' | ConvertFrom-Json
    Assert-NativeExit 'CLI retry backfill confirmation'
    $receiptIDs = @($receipt.items | Where-Object state -eq created | ForEach-Object run_id)
    $repeatIDs = @($again.items | Where-Object state -eq created | ForEach-Object run_id)
    if ($receipt.state -ne 'completed' -or $again.state -ne 'completed' -or $receiptIDs.Count -ne 3 -or
        @($receiptIDs | Sort-Object -Unique).Count -ne 3 -or ($receiptIDs -join ',') -ne ($repeatIDs -join ',') -or
        $partialIDs[0] -ne $receiptIDs[0] -or $partialIDs[1] -ne $receiptIDs[1]) {
        throw 'Backfill confirmation changed the three selected runs'
    }
    $readBatch = & $binary backfill-resume demo $preview.id | ConvertFrom-Json
    Assert-NativeExit 'CLI resume completed batch'
    $historical = & $binary run demo $receiptIDs[0] | ConvertFrom-Json
    Assert-NativeExit 'CLI read backfill run'
    $expectedOperation = ($preview.items[0].impact | Where-Object node -eq extract).operation_key
    $recordedOperation = ($historical.nodes | Where-Object key -eq extract).operation_key
    if ($readBatch.state -ne 'completed' -or $historical.run.revision -ne 1 -or $historical.run.trigger_kind -ne 'backfill' -or
        !$expectedOperation -or $expectedOperation -ne $recordedOperation) { throw 'Backfill run lost its frozen version or operation key' }
    Write-Output 'CLI selected three of seven historical occurrences on revision 1, resumed in a new process, and retried without new runs.'

    $recoveryReads = @(
        @('revision', 'demo', 'dispatch-report', '1'),
        @('revision', 'demo', 'dispatch-report', '2'),
        @('schedule-get', 'demo', 'dispatch-report', 'hourly-dispatch'),
        @('backfill-get', 'demo', $preview.id),
        @('run', 'demo', $receiptIDs[0]),
        @('run', 'demo', $created.run.id),
        @('run', 'demo', $shipment.run.id),
        @('queue-quota-get', 'demo', 'reports'),
        @('run-export', 'demo', $failedRun.run.id),
        @('run-export', 'demo', $rerun.run_id)
    )
    $originalEvidence = foreach ($check in $recoveryReads) { Read-RecoveryEvidence $check }
    $sourceAccount = $clientArgs[0..3] + '--user=workflow_test'
    $env:MYSQL_PWD = $appPassword
    $checksums = @(Get-FixtureChecksums $sourceAccount $database)
    $dump = Join-Path $fixture 'workflow-backup.sql'
    $backupTimer = [Diagnostics.Stopwatch]::StartNew()
    & $mysqldump @sourceAccount --single-transaction --quick --hex-blob --no-tablespaces --set-gtid-purged=OFF `
        --skip-add-drop-table --skip-add-locks "--result-file=$dump" $database
    Assert-NativeExit 'export logical MySQL backup'
    $backupTimer.Stop()
    $dumpHash = (Get-FileHash -LiteralPath $dump -Algorithm SHA256).Hash.ToLowerInvariant()
    $dumpBytes = (Get-Item -LiteralPath $dump).Length

    $recoveryDB = "${database}_restore"
    $recoveryPassword = [guid]::NewGuid().ToString('N')
    $recoveryPort = Get-LoopbackPort
    $recoveryClientArgs = @('--no-defaults', '--protocol=TCP', '--host=127.0.0.1', "--port=$recoveryPort", '--user=root')
    $null = New-Item -ItemType Directory -Path $recoveryDirectory
    $recoveryServer = Start-FixtureDatabase $recoveryDirectory $recoveryPort $recoveryDB workflow_restore $recoveryPassword $recoveryRootPassword
    $env:MYSQL_PWD = $recoveryRootPassword
    $recoveryVersion = Wait-FixtureDatabase $recoveryServer $recoveryClientArgs $recoveryDirectory
    $targetAccount = $recoveryClientArgs[0..3] + '--user=workflow_restore'
    $env:MYSQL_PWD = $recoveryPassword
    $beforeImport = & $mysql @targetAccount "--database=$recoveryDB" '--batch' '--skip-column-names' '--execute=SHOW TABLES'
    Assert-NativeExit 'verify empty restore database'
    if ($beforeImport) { throw 'The recovery database is not empty' }
    $restoreTimer = [Diagnostics.Stopwatch]::StartNew()
    $import = Start-Process -FilePath $mysql -ArgumentList ($targetAccount + @('--binary-mode', "--database=$recoveryDB")) `
        -RedirectStandardInput $dump -PassThru -Wait -WindowStyle Hidden
    $restoreTimer.Stop()
    if ($import.ExitCode -ne 0) { throw "MySQL backup import failed with code $($import.ExitCode)" }
    $restoredChecksums = @(Get-FixtureChecksums $targetAccount $recoveryDB)
    if (($checksums -join "`n") -cne ($restoredChecksums -join "`n")) { throw 'Restored fixture table checksums differ' }

    $env:WORKFLOW_MYSQL_DSN = "workflow_restore:${recoveryPassword}@tcp(127.0.0.1:$recoveryPort)/$recoveryDB"
    $restoredVersion = & $binary migrate | ConvertFrom-Json
    Assert-NativeExit 'verify restored schema version'
    if ($restoredVersion.schema_version -ne $migration.schema_version) { throw 'Restored schema history differs' }
    for ($index = 0; $index -lt $recoveryReads.Count; $index++) {
        if ((Read-RecoveryEvidence $recoveryReads[$index]) -cne $originalEvidence[$index]) {
            throw "Restored read differs for $($recoveryReads[$index] -join ' ')"
        }
    }
    $restoredReplay = & $binary trigger demo dispatch-report demo-001 examples/diamond-inputs.json | ConvertFrom-Json
    Assert-NativeExit 'replay a restored trigger'
    $restoredRerun = & $binary run-rerun demo $failedRun.run.id inspect-failure extract | ConvertFrom-Json
    Assert-NativeExit 'replay a restored rerun'
    $restoredWrite = & $binary trigger --revision 1 demo dispatch-report restore-smoke examples/diamond-inputs.json | ConvertFrom-Json
    Assert-NativeExit 'write a fresh run to the restored database'
    if (!$restoredReplay.replayed -or $restoredReplay.run.id -ne $created.run.id -or
        !$restoredRerun.replayed -or $restoredRerun.run_id -ne $rerun.run_id -or $restoredWrite.replayed -or
        $restoredWrite.run.id -le ($receiptIDs | Measure-Object -Maximum).Maximum -or $restoredWrite.run.revision -ne 1) {
        throw 'Restore lost request deduplication or could not allocate a new run'
    }
    Write-Output "MySQL $recoveryVersion backup and restore matched $($checksums.Count) table checksums and $($recoveryReads.Count) CLI reads."
    Write-Output "Backup bytes=$dumpBytes sha256=$dumpHash dump_ms=$($backupTimer.ElapsedMilliseconds) import_ms=$($restoreTimer.ElapsedMilliseconds). Replayed trigger and rerun retained their IDs; a new run was registered."
    $passed = $true
} finally {
    try {
        if ($console -and !$console.HasExited) {
            Stop-Process -Id $console.Id
            if (!$console.WaitForExit(5000)) { throw 'Fixture console failed to stop' }
        }
        if ($workerAPI -and !$workerAPI.HasExited) {
            Stop-Process -Id $workerAPI.Id
            if (!$workerAPI.WaitForExit(5000)) { throw 'Fixture Worker API failed to stop' }
        }
        foreach ($scanner in $scanners) {
            if (!$scanner.HasExited) {
                Stop-Process -Id $scanner.Id
                $null = $scanner.WaitForExit(5000)
            }
        }
        Stop-FixtureDatabase $recoveryServer $recoveryClientArgs $recoveryRootPassword
        Stop-FixtureDatabase $server $clientArgs $rootPassword
        foreach ($directory in @($fixture, $recoveryDirectory)) {
            $initSQL = Join-Path $directory 'init.sql'
            if (Test-Path -LiteralPath $initSQL) { Remove-Item -LiteralPath $initSQL }
        }
        if ($passed) {
            $resolved = (Resolve-Path -LiteralPath $fixture).Path
            if ((Split-Path $resolved -Parent) -ne $fixtureParent -or (Split-Path $resolved -Leaf) -ne "mysql-$key") {
                throw 'Unexpected fixture directory'
            }
            Remove-Item -LiteralPath $resolved -Recurse -Force
        } else {
            Write-Output "Fixture logs are in $fixture"
        }
    } finally {
        foreach ($entry in $savedEnvironment.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
        }
        Pop-Location
    }
}
