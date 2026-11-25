param([Parameter(Mandatory)][string]$MySQLBin)

$ErrorActionPreference = 'Stop'
$MySQLBin = (Resolve-Path -LiteralPath $MySQLBin).Path
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
    go test -count=1 -v ./...
    Assert-NativeExit 'Go tests'
    go vet ./...
    Assert-NativeExit 'Go vet'
    go build -o $binary ./cmd/workflow
    Assert-NativeExit 'build workflow CLI'

    $migration = & $binary migrate | ConvertFrom-Json
    Assert-NativeExit 'CLI migrate'
    if ($migration.schema_version -ne 1) { throw 'Wrong fixture schema version' }
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
    $passed = $true
} finally {
    try {
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
