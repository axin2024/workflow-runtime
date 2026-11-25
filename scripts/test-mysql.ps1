param([Parameter(Mandatory)][string]$MySQLBin, [string]$PreviousCLI)

$ErrorActionPreference = 'Stop'
$MySQLBin = (Resolve-Path -LiteralPath $MySQLBin).Path
if ($PreviousCLI) { $PreviousCLI = (Resolve-Path -LiteralPath $PreviousCLI).Path }
$project = Split-Path $PSScriptRoot -Parent
$fixtureParent = (New-Item -ItemType Directory -Path (Join-Path $project '.local') -Force).FullName
$key = [guid]::NewGuid().ToString('N').Substring(0, 12)
$fixture = (New-Item -ItemType Directory -Path (Join-Path $fixtureParent "mysql-$key")).FullName
$data = (New-Item -ItemType Directory -Path (Join-Path $fixture 'data')).FullName
$initSQL = Join-Path $fixture 'init.sql'
$binary = Join-Path $fixture 'workflow.exe'
$database = "workflow_test_$key"
$rootPassword = [guid]::NewGuid().ToString('N')
$appPassword = [guid]::NewGuid().ToString('N')
$server = $null
$scanners = @()
$passed = $false

function Assert-NativeExit([string]$Operation) {
    if ($LASTEXITCODE -ne 0) { throw "$Operation exited with code $LASTEXITCODE" }
}

$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
try {
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
} finally {
    $listener.Stop()
}

$savedEnvironment = @{}
foreach ($name in @('WORKFLOW_MYSQL_DSN', 'WORKFLOW_TEST_MYSQL_DSN', 'WORKFLOW_ACTOR', 'MYSQL_PWD')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$mysql = Join-Path $MySQLBin 'mysql.exe'
$mysqld = Join-Path $MySQLBin 'mysqld.exe'
$mysqladmin = Join-Path $MySQLBin 'mysqladmin.exe'
$clientArgs = @('--no-defaults', '--protocol=TCP', '--host=127.0.0.1', "--port=$port", '--user=root')
$serverArgs = @('--no-defaults', '--innodb-buffer-pool-size=32M', '--max-connections=48', '--mysqlx=OFF',
    ('--basedir="{0}"' -f (Split-Path $MySQLBin -Parent)), ('--datadir="{0}"' -f $data),
    ('--log-error="{0}/mysql.log"' -f $fixture))

Push-Location -LiteralPath $project
try {
    $init = Start-Process -FilePath $mysqld -ArgumentList ($serverArgs + '--initialize-insecure') -PassThru -Wait -WindowStyle Hidden
    if ($init.ExitCode -ne 0) { throw 'MySQL initialization failed' }
    @"
ALTER USER 'root'@'localhost' IDENTIFIED BY '$rootPassword';
CREATE DATABASE $database CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'workflow_test'@'127.0.0.1' IDENTIFIED BY '$appPassword';
GRANT ALL PRIVILEGES ON $database.* TO 'workflow_test'@'127.0.0.1';
"@ | Set-Content -LiteralPath $initSQL -Encoding ascii
    $server = Start-Process -FilePath $mysqld -ArgumentList ($serverArgs + @('--bind-address=127.0.0.1', "--port=$port", ('--init-file="{0}"' -f $initSQL))) -PassThru -WindowStyle Hidden
    $env:MYSQL_PWD = $rootPassword
    $version = $null
    for ($i = 0; $i -lt 40; $i++) {
        if ($server.HasExited) { throw 'MySQL exited before accepting connections' }
        $version = & $mysql @clientArgs '--batch' '--skip-column-names' '--execute=SELECT VERSION()' 2>$null
        if ($LASTEXITCODE -eq 0 -and $version) { break }
        Start-Sleep -Milliseconds 250
    }
    if (!$version -or [string]$version -notmatch '^8\.4\.') { throw 'Could not connect to a MySQL 8.4 test server' }
    Remove-Item -LiteralPath $initSQL
    Write-Output "Isolated MySQL $version"

    $env:WORKFLOW_TEST_MYSQL_DSN = "workflow_test:${appPassword}@tcp(127.0.0.1:$port)/$database"
    $env:WORKFLOW_MYSQL_DSN = $env:WORKFLOW_TEST_MYSQL_DSN
    $env:WORKFLOW_ACTOR = 'fixture-operator'
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
    $passed = $true
} finally {
    try {
        foreach ($scanner in $scanners) {
            if (!$scanner.HasExited) {
                Stop-Process -Id $scanner.Id
                $null = $scanner.WaitForExit(5000)
            }
        }
        if ($server -and !$server.HasExited) {
            $env:MYSQL_PWD = $rootPassword
            try {
                & $mysqladmin @clientArgs shutdown 2>$null
            } finally {
                if (!$server.WaitForExit(5000)) {
                    Stop-Process -Id $server.Id
                    if (!$server.WaitForExit(5000)) { throw 'Fixture MySQL failed to stop' }
                }
            }
        }
        if (Test-Path -LiteralPath $initSQL) { Remove-Item -LiteralPath $initSQL }
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
