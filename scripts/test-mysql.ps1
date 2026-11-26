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

function Test-DemoFailover([string]$Endpoint) {
    $seed = & $binary demo-init | ConvertFrom-Json
    Assert-NativeExit 'initialize the bundled demo'
    $reseed = & $binary demo-init | ConvertFrom-Json
    Assert-NativeExit 'repeat demo initialization'
    if ($seed.replayed -or !$reseed.replayed -or $seed.run.id -ne $reseed.run.id -or $seed.run.revision -ne 1) {
        throw 'Repeated demo setup did not reuse its original run'
    }
    $demoA = & $binary worker-register workflow-demo worker-a 1 failover:aggregate | ConvertFrom-Json
    Assert-NativeExit 'register demo Worker A'
    $demoB = & $binary worker-register workflow-demo worker-b 1 failover:extract failover:aggregate failover:inspect failover:report | ConvertFrom-Json
    Assert-NativeExit 'register demo Worker B'
    $sourceDSN = $env:WORKFLOW_MYSQL_DSN
    $sourceTestDSN = $env:WORKFLOW_TEST_MYSQL_DSN
    $sourceMySQLPassword = $env:MYSQL_PWD
    $pausedWorker = $null
    try {
        $env:WORKFLOW_MYSQL_DSN = $null
        $env:WORKFLOW_TEST_MYSQL_DSN = $null
        $env:MYSQL_PWD = $null
        $env:WORKFLOW_WORKER_TOKEN = $demoB.token
        $first = & $binary worker --once --endpoint $Endpoint | ConvertFrom-Json
        Assert-NativeExit 'execute the sample extract as Worker B'
        if ($first.run_id -ne $seed.run.id -or $first.node -ne 'extract') { throw 'Unexpected initial demo task' }

        $start = [Diagnostics.ProcessStartInfo]::new($binary)
        $start.UseShellExecute = $false
        $start.CreateNoWindow = $true
        $start.RedirectStandardInput = $true
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        foreach ($argument in @('demo-worker', '--endpoint', $Endpoint, '--lease-seconds', '6')) { $start.ArgumentList.Add($argument) }
        $start.Environment['WORKFLOW_WORKER_TOKEN'] = $demoA.token
        $pausedWorker = [Diagnostics.Process]::Start($start)
        $paused = $pausedWorker.StandardOutput.ReadLineAsync().WaitAsync([TimeSpan]::FromSeconds(10)).GetAwaiter().GetResult() | ConvertFrom-Json
        if ($paused.phase -ne 'paused' -or $paused.run_id -ne $seed.run.id -or $paused.node -ne 'aggregate' -or $paused.epoch -ne 1) {
            throw 'Worker A did not pause with the original aggregate lease'
        }
        $branch = & $binary worker --once --endpoint $Endpoint | ConvertFrom-Json
        Assert-NativeExit 'execute the independent branch while Worker A is paused'
        $beforeExpiry = & $binary worker --once --endpoint $Endpoint | ConvertFrom-Json
        Assert-NativeExit 'observe the join before the aggregate lease expires'
        if ($branch.node -ne 'inspect' -or $null -ne $beforeExpiry) { throw 'The report became available before both branches completed' }

        $limit = [Diagnostics.Stopwatch]::StartNew()
        do {
            Start-Sleep -Milliseconds 150
            $takeover = & $binary worker --once --endpoint $Endpoint | ConvertFrom-Json
            Assert-NativeExit 'take over the paused demo task'
        } until ($takeover -or $limit.Elapsed.TotalSeconds -ge 12)
        if ($takeover.run_id -ne $paused.run_id -or $takeover.node -ne $paused.node -or $takeover.epoch -ne 2 -or
            $takeover.operation_key -ne $paused.operation_key -or $takeover.state -ne 'succeeded') {
            throw 'Worker B did not complete the same aggregate node under a new lease'
        }
        $pausedWorker.StandardInput.WriteLine()
        $pausedWorker.StandardInput.Close()
        $resumed = $pausedWorker.StandardOutput.ReadLineAsync().WaitAsync([TimeSpan]::FromSeconds(10)).GetAwaiter().GetResult() | ConvertFrom-Json
        if (!$pausedWorker.WaitForExit(5000) -or $pausedWorker.ExitCode -ne 0 -or
            $resumed.phase -ne 'resumed' -or $resumed.epoch -ne 1 -or $resumed.operation_key -ne $paused.operation_key -or
            $resumed.outcome -ne 'lease_lost') { throw 'The old aggregate completion was not rejected' }
        $join = & $binary worker --once --endpoint $Endpoint | ConvertFrom-Json
        Assert-NativeExit 'execute the report join after takeover'
        $extra = & $binary worker --once --endpoint $Endpoint | ConvertFrom-Json
        Assert-NativeExit 'verify no second report claim'
        if ($join.node -ne 'report' -or $join.epoch -ne 1 -or $null -ne $extra) { throw 'Expected exactly one report execution' }
    } finally {
        $env:WORKFLOW_WORKER_TOKEN = $null
        $env:WORKFLOW_MYSQL_DSN = $sourceDSN
        $env:WORKFLOW_TEST_MYSQL_DSN = $sourceTestDSN
        $env:MYSQL_PWD = $sourceMySQLPassword
        if ($pausedWorker) {
            if (!$pausedWorker.HasExited) { $pausedWorker.Kill(); $null = $pausedWorker.WaitForExit(5000) }
            $pausedWorker.Dispose()
        }
    }
    $export = & $binary run-export workflow-demo $seed.run.id | ConvertFrom-Json
    Assert-NativeExit 'read the demo report from MySQL'
    $aggregate = $export.detail.nodes | Where-Object key -eq aggregate
    $report = $export.detail.nodes | Where-Object key -eq report
    if ($export.detail.run.state -ne 'succeeded' -or $aggregate.attempts.Count -ne 2 -or
        $aggregate.attempts[0].worker -ne 'worker-a' -or $aggregate.attempts[0].failure_code -ne 'lease_expired' -or
        $aggregate.attempts[1].worker -ne 'worker-b' -or $aggregate.attempts[1].state -ne 'succeeded' -or
        $report.attempts.Count -ne 1 -or $report.outputs.summary -ne '12 dispatch records (passed)' -or
        @($report.dependencies | Where-Object status -eq satisfied).Count -ne 2) {
        throw 'MySQL did not retain both attempts or exactly one report with its two dependency decisions'
    }
    Write-Output "Public demo run=$($seed.run.id) input_sha256=$($export.detail.input_digest) output_sha256=$($report.output_digest). Worker A expired, Worker B completed at epoch 2, the old completion was rejected, and report executed once."
}

function Test-ExternalTriggers([string]$Endpoint, [string]$WorkerToken) {
    foreach ($workspace in @('ingress-test', 'ingress-foreign')) {
        & $binary workspace-create $workspace | Out-Null
        Assert-NativeExit 'create external trigger workspace'
        & $binary publish $workspace 0 examples/diamond.json | Out-Null
        Assert-NativeExit 'publish workflow for external triggers'
    }
    $owner = & $binary trigger-grant ingress-test dispatch-report report-service | ConvertFrom-Json
    Assert-NativeExit 'grant trigger credential'
    $peer = & $binary trigger-grant ingress-test dispatch-report other-service | ConvertFrom-Json
    Assert-NativeExit 'grant peer trigger credential'
    $foreign = & $binary trigger-grant ingress-foreign dispatch-report report-service | ConvertFrom-Json
    Assert-NativeExit 'grant same actor in another workspace'
    $url = "$Endpoint/v1/triggers"
    $headers = @{Authorization = "Bearer $($owner.token)"}
    $request = '{"key":"workflow/weekly:2026-W44","inputs":{"source":"trigger-fixture","rows":12}}'
    $created = Invoke-WebRequest -Method Post -Uri $url -Headers $headers -ContentType 'application/json' -Body $request
    $run = $created.Content | ConvertFrom-Json
    $runID = $run.run.id
    if ($created.StatusCode -ne 202 -or $run.replayed -or $run.run.dag -ne 'dispatch-report' -or $run.run.revision -ne 1 -or
        $run.run.state -ne 'queued' -or $run.run.PSObject.Properties.Name -contains 'inputs' -or
        $created.Headers.Location[0] -ne "/v1/triggers/$runID") { throw 'External trigger did not return a narrow committed receipt' }
    $lookup = Invoke-RestMethod -Uri "$url/$runID" -Headers $headers
    $byKey = Invoke-RestMethod -Uri "$url`?key=workflow%2Fweekly%3A2026-W44" -Headers $headers
    $replay = Invoke-WebRequest -Method Post -Uri $url -Headers $headers -ContentType 'application/json' `
        -Body '{"key":"workflow/weekly:2026-W44","inputs":{"rows":12,"source":"trigger-fixture"}}'
    $retried = $replay.Content | ConvertFrom-Json
    if ($lookup.id -ne $runID -or $byKey.id -ne $runID -or $replay.StatusCode -ne 200 -or !$retried.replayed -or $retried.run.id -ne $runID) {
        throw 'External trigger could not recover its receipt by key, run ID, and replay'
    }
    $conflict = Invoke-WebRequest -Method Post -Uri $url -Headers $headers -ContentType 'application/json' -SkipHttpErrorCheck `
        -Body $request.Replace('"rows":12', '"rows":13')
    $wrongType = Invoke-WebRequest -Method Post -Uri $url -Headers $headers -ContentType 'application/json' -SkipHttpErrorCheck `
        -Body '{"key":"wrong-type","inputs":{"rows":"12","source":"test"}}'
    $override = Invoke-WebRequest -Method Post -Uri $url -Headers $headers -ContentType 'application/json' -SkipHttpErrorCheck `
        -Body '{"key":"override","workspace":"ingress-foreign","dag":"other","inputs":{}}'
    if ($conflict.StatusCode -ne 409 -or ($conflict.Content | ConvertFrom-Json).run_id -ne $runID -or
        $wrongType.StatusCode -ne 400 -or ($wrongType.Content | ConvertFrom-Json).field -ne 'inputs.rows' -or $override.StatusCode -ne 400) {
        throw 'External trigger accepted conflicting, invalid, or forged scope parameters'
    }
    foreach ($credential in @($peer, $foreign)) {
        $unowned = Invoke-WebRequest -Uri "$url/$runID" -Headers @{Authorization = "Bearer $($credential.token)"} -SkipHttpErrorCheck
        if ($unowned.StatusCode -ne 404) { throw 'A different actor or workspace read the trigger receipt' }
    }
    $sameKey = Invoke-WebRequest -Method Post -Uri $url -Headers @{Authorization = "Bearer $($peer.token)"} `
        -ContentType 'application/json' -Body $request -SkipHttpErrorCheck
    $workerDenied = Invoke-WebRequest -Uri "$url/$runID" -Headers @{Authorization = "Bearer $WorkerToken"} -SkipHttpErrorCheck
    $triggerDenied = Invoke-WebRequest -Method Post -Uri "$Endpoint/v1/worker/claim" -Headers $headers `
        -ContentType 'application/json' -Body '{"tasks":["extract"],"lease_seconds":1}' -SkipHttpErrorCheck
    if ($sameKey.StatusCode -ne 409 -or ($sameKey.Content | ConvertFrom-Json).PSObject.Properties.Name -contains 'run_id' -or
        $workerDenied.StatusCode -ne 401 -or $triggerDenied.StatusCode -ne 401) { throw 'External triggers crossed a credential boundary' }

    $concurrentRequest = $request.Replace('workflow/weekly:2026-W44', 'parallel-delivery')
    $concurrent = @(1..4 | ForEach-Object -ThrottleLimit 4 -Parallel {
        $response = Invoke-WebRequest -Method Post -Uri $using:url -Headers $using:headers -ContentType 'application/json' -Body $using:concurrentRequest
        $result = $response.Content | ConvertFrom-Json
        [pscustomobject]@{status = $response.StatusCode; id = $result.run.id; replayed = $result.replayed}
    })
    if ($concurrent.Count -ne 4 -or @($concurrent | Where-Object status -eq 202).Count -ne 1 -or
        @($concurrent | Where-Object replayed).Count -ne 3 -or @($concurrent.id | Sort-Object -Unique).Count -ne 1) {
        throw 'Concurrent webhook deliveries allocated multiple runs'
    }
    $history = & $binary run ingress-test $runID | ConvertFrom-Json
    Assert-NativeExit 'read audited external run through CLI'
    if ($history.run.trigger_kind -ne 'external' -or @($history.audit | Where-Object { $_.actor -eq 'report-service' }).Count -ne 2) {
        throw 'The external actor was missing from creation or conflict audit'
    }
    & $binary run-cancel ingress-test $runID external-test-stop 'Requested by operator' | Out-Null
    Assert-NativeExit 'cancel external run as operator'
    & $binary trigger-revoke ingress-test dispatch-report report-service | Out-Null
    Assert-NativeExit 'revoke external trigger credential'
    $revoked = Invoke-WebRequest -Uri "$url/$runID" -Headers $headers -SkipHttpErrorCheck
    $revokedPost = Invoke-WebRequest -Method Post -Uri $url -Headers $headers -ContentType 'application/json' -Body $request -SkipHttpErrorCheck
    $rotated = & $binary trigger-grant ingress-test dispatch-report report-service | ConvertFrom-Json
    Assert-NativeExit 'reissue trigger credential for the same actor'
    $afterRotation = Invoke-RestMethod -Uri "$url/$runID" -Headers @{Authorization = "Bearer $($rotated.token)"}
    $oldStillDenied = Invoke-WebRequest -Uri "$url/$runID" -Headers $headers -SkipHttpErrorCheck
    if ($revoked.StatusCode -ne 401 -or $revokedPost.StatusCode -ne 401 -or $oldStillDenied.StatusCode -ne 401 -or
        $afterRotation.id -ne $runID -or $afterRotation.state -ne 'canceled' -or !$afterRotation.finished_at) {
        throw 'Credential revoke, rotation, or persisted state readback failed'
    }
    Write-Output 'External trigger HTTP receipts survived replay, concurrent delivery, cancellation and credential rotation. Actor, tenant, and worker boundaries passed.'
}

function New-PreviousCallbackFixture([string]$CLI) {
    & $CLI workspace-create upgrade | Out-Null
    Assert-NativeExit 'create schema 10 callback workspace'
    & $CLI publish upgrade 0 examples/diamond.json | Out-Null
    Assert-NativeExit 'publish schema 10 callback fixture'
    $grant = & $CLI trigger-grant upgrade dispatch-report legacy-caller | ConvertFrom-Json
    Assert-NativeExit 'grant schema 10 external trigger'
    $apiPort = Get-LoopbackPort
    $origin = "http://127.0.0.1:$apiPort"
    $oldAPI = Start-Process -FilePath $CLI -ArgumentList @('worker-api', '--listen', "127.0.0.1:$apiPort") -WindowStyle Hidden -PassThru `
        -RedirectStandardError (Join-Path $fixture 'previous-api.log')
    try {
        $healthy = $false
        for ($i = 0; $i -lt 40; $i++) {
            if ($oldAPI.HasExited) { throw 'Schema 10 API exited before accepting connections' }
            try {
                if ((Invoke-WebRequest -Uri "$origin/healthz" -TimeoutSec 1).StatusCode -eq 204) { $healthy = $true; break }
            } catch { Start-Sleep -Milliseconds 100 }
        }
        if (!$healthy) { throw 'Schema 10 API failed its health check' }
        $headers = @{ Authorization = "Bearer $($grant.token)" }
        $terminal = Invoke-RestMethod -Method Post -Uri "$origin/v1/triggers" -Headers $headers -ContentType 'application/json' `
            -Body '{"key":"legacy-terminal","inputs":{"rows":1,"source":"upgrade-fixture"}}'
        $pending = Invoke-RestMethod -Method Post -Uri "$origin/v1/triggers" -Headers $headers -ContentType 'application/json' `
            -Body '{"key":"legacy-pending","inputs":{"rows":1,"source":"upgrade-fixture"}}'
        $stopped = & $CLI run-cancel upgrade $terminal.run.id legacy-stop 'synthetic migration fixture' | ConvertFrom-Json
        Assert-NativeExit 'settle pre-upgrade external run'
        if (!$stopped.finished_at -or !$pending.run.id -or $terminal.run.id -eq $pending.run.id) { throw 'Invalid pre-upgrade external runs' }
        return @{ terminal = [long]$terminal.run.id; pending = [long]$pending.run.id }
    } finally {
        if (!$oldAPI.HasExited) {
            Stop-Process -Id $oldAPI.Id
            if (!$oldAPI.WaitForExit(5000)) { throw 'Schema 10 API failed to stop' }
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
    $previousSchema = 0
    if ($CapacityLoad) { $env:WORKFLOW_CAPACITY_LOAD = '1' }
    if ($PreviousCLI) {
        $previousMigration = & $PreviousCLI migrate | ConvertFrom-Json
        Assert-NativeExit 'previous CLI migrate'
        $previousSchema = $previousMigration.schema_version
        switch ($previousSchema) {
            { $_ -in @(2, 11) } {
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
            10 { $previousCallbacks = New-PreviousCallbackFixture $PreviousCLI }
            default { throw 'Previous CLI must use schema 2, 10 or 11' }
        }
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
    if ($previousSchema -in @(2, 11)) {
        $oldRun = & $binary run upgrade $previousRun[0] | ConvertFrom-Json
        Assert-NativeExit 'read scheduled run after schema upgrade'
        $oldPreview = & $binary backfill-preview upgrade dispatch-report hourly-dispatch 1 '2026-11-01T01:00:00+08:00' '2026-11-01T03:00:00+08:00' | ConvertFrom-Json
        Assert-NativeExit 'preview pre-upgrade occurrence'
        if ($oldRun.run.id -ne $previousRun[0] -or $oldRun.run.trigger_kind -ne 'scheduled' -or
            $oldRun.version.graph.digest -ne $previousGraph.graph.digest -or $oldPreview.items.Count -ne 3 -or
            $oldPreview.items[0].state -ne 'already_registered' -or $oldPreview.items[0].baseline_run_id -ne $previousRun[0]) {
            throw 'Schema upgrade changed the existing scheduled occurrence'
        }
        Write-Output "Previous schema $previousSchema data survived the upgrade and was found as the existing backfill baseline."
    }
    if ($previousSchema -eq 10) {
        $oldCallback = & $binary callback-status upgrade dispatch-report legacy-caller $previousCallbacks.terminal | ConvertFrom-Json
        Assert-NativeExit 'read backfilled callback after schema upgrade'
        $pendingCount = & $mysql @clientArgs "--database=$database" '--batch' '--skip-column-names' `
            "--execute=SELECT COUNT(*) FROM callback_deliveries WHERE run_id=$($previousCallbacks.pending)"
        Assert-NativeExit 'check that a running upgrade fixture remains unqueued for callback'
        if ($oldCallback.run_id -ne $previousCallbacks.terminal -or !$oldCallback.event_id -or $oldCallback.attempts -ne 0 -or
            $oldCallback.delivered_at -or $pendingCount -ne '0') { throw 'Terminal callback backfill selected the wrong pre-upgrade runs' }
        Write-Output "Schema 10 terminal run $($oldCallback.run_id) gained an unattempted callback. Nonterminal run $($previousCallbacks.pending) remained out of the queue."
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
    Test-DemoFailover $apiOrigin
    Test-ExternalTriggers $apiOrigin $workerA.token
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
    $exportPath = '/runs/report.json?dag=console-dispatch&state=queued&from=2025-11-01T01%3A00%3A00%2B08%3A00'
    $runExport = Invoke-WebRequest -Uri "$consoleOrigin$exportPath" -WebSession $ops
    $runList = $runExport.Content | ConvertFrom-Json
    $foreignList = Invoke-RestMethod -Uri "$consoleOrigin$exportPath" -WebSession $readOnly
    $olderList = Invoke-RestMethod -Uri "$consoleOrigin$exportPath&before=$registeredID" -WebSession $ops
    $invalidFilters = Invoke-WebRequest -Uri "$consoleOrigin/runs/report.json?state=invalid" -WebSession $ops -SkipHttpErrorCheck
    if ($filtered.Content -notmatch 'href="/runs/report.json\?dag=console-dispatch&amp;state=queued"' -or
        $runExport.Headers['Content-Disposition'] -notmatch 'attachment; filename="run-list.json"' -or
        $runList.format -ne 'workflow-runtime/run-list-v1' -or !$runList.generated_at -or !$runList.generated_by -or
        $runList.workspace -ne 'demo' -or $runExport.Content -notmatch '"from": "2025-10-31T17:00:00Z"' -or
        $runList.filter.dag -ne 'console-dispatch' -or $runList.filter.state -ne 'queued' -or
        $runList.items.Count -ne 1 -or $runList.items[0].id -ne $registeredID -or
        $foreignList.workspace -eq 'demo' -or $foreignList.items.Count -ne 0 -or $olderList.filter.before -ne $registeredID -or
        $olderList.items.Count -ne 0 -or $invalidFilters.StatusCode -ne 400) {
        throw 'Run list report failed scope, filtering, attachment, or version checks'
    }
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
    $foreignBatchReport = Invoke-WebRequest -Uri "$consoleOrigin/backfills/$batchID/report.json" -WebSession $readOnly -SkipHttpErrorCheck
    if ($selectionConflict.StatusCode -ne 409 -or $foreignBatch.StatusCode -ne 404 -or $foreignBatchReport.StatusCode -ne 404) { throw 'Backfill selection or workspace boundary was bypassed' }
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
    $batchReport = Invoke-RestMethod -Uri "$consoleOrigin/backfills/$batchID/report.json" -WebSession $writer
    if ($replayedBatch.Content -notmatch ('href="/backfills/' + $batchID + '/report.json"') -or
        $batchReport.format -ne 'workflow-runtime/backfill-report-v1' -or !$batchReport.generated_at -or !$batchReport.generated_by -or
        $batchReport.selection_digest -notmatch '^[0-9a-f]{64}$' -or $batchReport.batch.items.Count -ne 7 -or
        $batchReport.batch.revision -ne 1 -or $batchReport.runs.Count -ne 3 -or
        ((@($batchReport.runs | ForEach-Object run_id)) -join ',') -ne ($finalIDs -join ',')) {
        throw 'Backfill report did not preserve the requested historical runs and selection'
    }
    Write-Output 'Console run-list and backfill exports preserve filters, scoped authorship, version, and persisted receipts.'

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

    $retentionGraph = Join-Path $fixture 'retention-workflow.json'
    [IO.File]::WriteAllText($retentionGraph, @'
{"key":"retention-workflow","nodes":[
  {"key":"produce","task":"produce","queue":"retention","effect":"none","timeout_seconds":60,"retry":{"max_attempts":1}},
  {"key":"finish","task":"finish","queue":"retention","effect":"none","timeout_seconds":60,"retry":{"max_attempts":1},
   "depends_on":[{"node":"produce","when":"succeeded"}]}]}
'@)
    $retentionInputs = Join-Path $fixture 'retention-inputs.json'
    [IO.File]::WriteAllText($retentionInputs, '{}')
    & $binary publish demo 0 $retentionGraph | Out-Null
    Assert-NativeExit 'publish log retention fixture'
    $logWorker = & $binary worker-register demo retention-worker 1 retention:produce retention:finish | ConvertFrom-Json
    Assert-NativeExit 'register log retention worker'
    $logHeaders = @{ Authorization = "Bearer $($logWorker.token)" }
    $logRuns = @{}
    $logCompletions = @{}
    $logText = 'retention fixture log body'
    foreach ($name in @('expired-one', 'expired-two', 'fresh', 'unfinished')) {
        $logRuns[$name] = (& $binary trigger demo retention-workflow $name $retentionInputs | ConvertFrom-Json).run.id
        Assert-NativeExit 'trigger a log retention test run'
        $tasks = if ($name -eq 'unfinished') { @('produce') } else { @('produce', 'finish') }
        foreach ($taskName in $tasks) {
            $leaseQuery = @{ tasks = @($taskName); lease_seconds = 30 } | ConvertTo-Json -Compress
            $leased = (Invoke-RestMethod -Method Post -Uri "$apiOrigin/v1/worker/claim" -Headers $logHeaders -ContentType 'application/json' -Body $leaseQuery).task
            if ($leased.run_id -ne $logRuns[$name] -or $leased.node -ne $taskName) { throw 'Wrong log retention lease' }
            $payload = @{
                lease = @{ run_id = $leased.run_id; node = $leased.node; epoch = $leased.epoch; operation_key = $leased.operation_key }
                success = $true
            }
            if ($taskName -eq 'produce') { $payload.log_excerpt = "$logText $name" }
            $body = $payload | ConvertTo-Json -Depth 4 -Compress
            if ($taskName -eq 'produce') { $logCompletions[$name] = $body }
            $accepted = Invoke-RestMethod -Method Post -Uri "$apiOrigin/v1/worker/complete" -Headers $logHeaders -ContentType 'application/json' -Body $body
            if ($accepted.state -ne 'succeeded') { throw 'Log fixture completion failed' }
        }
    }
    $oldAttempts = @($logRuns['expired-one'], $logRuns['expired-two'], $logRuns.unfinished) -join ','
    $oldRuns = @($logRuns['expired-one'], $logRuns['expired-two']) -join ','
    & $mysql @clientArgs "--database=$database" "--execute=UPDATE attempts SET finished_at=UTC_TIMESTAMP(6)-INTERVAL 2 DAY WHERE run_id IN ($oldAttempts); UPDATE retention_log_queue SET finished_at=UTC_TIMESTAMP(6)-INTERVAL 2 DAY WHERE run_id IN ($oldAttempts); UPDATE runs SET finished_at=UTC_TIMESTAMP(6)-INTERVAL 2 DAY WHERE id IN ($oldRuns);"
    Assert-NativeExit 'age only the selected synthetic log attempts'
    $retentionPage = Invoke-WebRequest -Uri "$consoleOrigin/retention" -WebSession $ops
    $deniedRetention = Invoke-WebRequest -Uri "$consoleOrigin/retention/report.json" -WebSession $readOnly -SkipHttpErrorCheck
    if ($retentionPage.Content -match 'action="/retention/cleanup"' -or $deniedRetention.StatusCode -ne 403) { throw 'Retention default or role boundary failed' }
    $beforeClear = Invoke-RestMethod -Uri "$consoleOrigin/runs/$($logRuns['expired-one'])/report.json" -WebSession $ops
    $savedPolicy = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/retention/policy" -WebSession $ops -ContentType 'application/x-www-form-urlencoded' `
        -Body @{ csrf = $csrf; generation = '0'; days = '1' }
    $expiredReport = Invoke-RestMethod -Uri "$consoleOrigin/runs/$($logRuns['expired-one'])/report.json" -WebSession $ops
    $expiredLog = ($expiredReport.detail.nodes | Where-Object key -eq produce).attempts[0]
    if (!$expiredLog.log_expired -or $expiredLog.log_excerpt -or !$expiredLog.log_sha256 -or $expiredLog.log_cleared_at) {
        throw 'Expired log was still readable before cleanup'
    }
    $cleanupKey = [regex]::Match($savedPolicy.Content, 'name="key" value="(cleanup-[a-z2-7]+)"').Groups[1].Value
    $retentionForm = @{ csrf = $csrf; key = $cleanupKey; generation = '1'; limit = '1' }
    $unused = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/retention/cleanup" -WebSession $ops -ContentType 'application/x-www-form-urlencoded' -Body $retentionForm
    $unused = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/retention/cleanup" -WebSession $ops -ContentType 'application/x-www-form-urlencoded' -Body $retentionForm
    $savedJobs = Invoke-RestMethod -Uri "$consoleOrigin/retention/report.json" -WebSession $ops
    if ($savedJobs.format -ne 'workflow.log-retention.v1' -or $savedJobs.policy.generation -ne 1 -or $savedJobs.policy.days -ne 1 -or
        $savedJobs.jobs.Count -ne 1 -or $savedJobs.jobs[0].cleared.Count -ne 1 -or !$savedJobs.jobs[0].has_more -or
        $savedJobs.jobs[0].cleared[0].run_id -ne $logRuns['expired-one']) { throw 'Retention receipt or replay failed' }
    $afterClear = Invoke-RestMethod -Uri "$consoleOrigin/runs/$($logRuns['expired-one'])/report.json" -WebSession $ops
    $beforeLog = ($beforeClear.detail.nodes | Where-Object key -eq produce).attempts[0]
    $afterLog = ($afterClear.detail.nodes | Where-Object key -eq produce).attempts[0]
    $clearedView = Invoke-WebRequest -Uri "$consoleOrigin/runs/$($logRuns['expired-one'])" -WebSession $ops
    $completionReplay = Invoke-RestMethod -Method Post -Uri "$apiOrigin/v1/worker/complete" -Headers $logHeaders -ContentType 'application/json' -Body $logCompletions['expired-one']
    if (!$beforeLog.log_excerpt -or $afterLog.log_excerpt -or !$afterLog.log_cleared_at -or $beforeLog.log_sha256 -ne $afterLog.log_sha256 -or
        $afterLog.result_digest -ne $beforeLog.result_digest -or $clearedView.Content -notmatch '日志正文已清理' -or
        $clearedView.Content -match $logText -or !$completionReplay.replayed) { throw 'Cleared log lost history or completion identity' }
    $nextExpired = Invoke-RestMethod -Uri "$consoleOrigin/runs/$($logRuns['expired-two'])/report.json" -WebSession $ops
    $nextLog = ($nextExpired.detail.nodes | Where-Object key -eq produce).attempts[0]
    if (!$nextLog.log_expired -or $nextLog.log_cleared_at) { throw 'First batch should leave the second expired log pending' }
    foreach ($name in @('fresh', 'unfinished')) {
        $remainingLog = Invoke-RestMethod -Uri "$consoleOrigin/runs/$($logRuns[$name])/report.json" -WebSession $ops
        if (($remainingLog.detail.nodes | Where-Object key -eq produce).attempts[0].log_excerpt -ne "$logText $name") {
            throw "Cleanup removed a log outside this batch ($name)"
        }
    }
    $nextPage = Invoke-WebRequest -Uri "$consoleOrigin/retention" -WebSession $ops
    $retentionForm.key = [regex]::Match($nextPage.Content, 'name="key" value="(cleanup-[a-z2-7]+)"').Groups[1].Value
    $retentionForm.limit = '128'
    $unused = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/retention/cleanup" -WebSession $ops -ContentType 'application/x-www-form-urlencoded' -Body $retentionForm
    $lastBatch = (Invoke-RestMethod -Uri "$consoleOrigin/retention/report.json" -WebSession $ops).jobs[0]
    if ($lastBatch.has_more -or $lastBatch.cleared.Count -ne 1 -or $lastBatch.cleared[0].run_id -ne $logRuns['expired-two']) {
        throw 'Final cleanup did not isolate the remaining expired terminal log'
    }
    $disabled = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/retention/policy" -WebSession $ops -ContentType 'application/x-www-form-urlencoded' `
        -Body @{ csrf = $csrf; generation = '1'; days = '0' }
    $retentionForm.key = 'cleanup-bbbbbbbbbbbbbbbbbbbbbbbbbb'
    $staleCleanup = Invoke-WebRequest -Method Post -Uri "$consoleOrigin/retention/cleanup" -WebSession $ops -ContentType 'application/x-www-form-urlencoded' `
        -Body $retentionForm -SkipHttpErrorCheck
    if ($disabled.Content -match 'action="/retention/cleanup"' -or $staleCleanup.StatusCode -ne 409) { throw 'Stale retention generation was accepted' }
    Write-Output 'Console retention cleared two expired logs in separate batches, preserved digests and completion replay, and left fresh and unfinished logs intact.'

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
