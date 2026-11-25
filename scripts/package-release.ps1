param(
    [Parameter(Mandatory)][ValidatePattern('^v[0-9]+\.[0-9]+\.[0-9]+$')][string]$Version,
    [string]$Revision = 'HEAD',
    [string]$OutputDirectory
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$repository = Split-Path $PSScriptRoot -Parent
if (!$OutputDirectory) { $OutputDirectory = Join-Path $repository ".local/release-$Version" }
$destination = (New-Item -ItemType Directory -Path $OutputDirectory -Force).FullName
$commit = (git -C $repository rev-parse --verify --end-of-options "$Revision^{commit}").Trim()
$scratch = Join-Path $destination "build-$([guid]::NewGuid().ToString('N'))"
$source = Join-Path $scratch 'source'
$null = New-Item -ItemType Directory -Path $source
$sourceArchive = Join-Path $scratch 'source.zip'
$platforms = @(
    @{ name = 'windows-amd64'; os = 'windows'; command = 'workflow.exe' },
    @{ name = 'linux-amd64'; os = 'linux'; command = 'workflow' }
)
$buildVariables = 'GOOS', 'GOARCH', 'CGO_ENABLED'
$originalEnvironment = @{}
foreach ($variable in $buildVariables) { $originalEnvironment[$variable] = [Environment]::GetEnvironmentVariable($variable) }

try {
    git -C $repository archive --format=zip "--output=$sourceArchive" $commit
    Expand-Archive -LiteralPath $sourceArchive -DestinationPath $source
    $goVersion = go env GOVERSION
    $env:GOARCH = 'amd64'
    $env:CGO_ENABLED = '0'
    $artifacts = @()
    foreach ($platform in $platforms) {
        $env:GOOS = $platform.os
        $name = "workflow-runtime-$Version-$($platform.name)"
        $stage = (New-Item -ItemType Directory -Path (Join-Path $scratch $name)).FullName
        foreach ($entry in @('LICENSE', 'THIRD_PARTY_NOTICES.md', 'README.md', 'CHANGELOG.md', 'SECURITY.md', 'licenses', 'docs', 'examples')) {
            Copy-Item -LiteralPath (Join-Path $source $entry) -Destination $stage -Recurse
        }
        $executable = Join-Path $stage $platform.command
        Push-Location -LiteralPath $source
        try {
            go build -trimpath -buildvcs=false -o $executable ./cmd/workflow
        } finally {
            Pop-Location
        }
        $manifest = [ordered]@{
            version = $Version
            source_commit = $commit
            platform = $platform.name
            go_version = $goVersion
            executable = $platform.command
            executable_sha256 = (Get-FileHash -LiteralPath $executable -Algorithm SHA256).Hash.ToLowerInvariant()
        }
        $manifest | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $stage 'BUILD.json') -Encoding utf8NoBOM
        $archive = Join-Path $scratch "$name.zip"
        Compress-Archive -LiteralPath $stage -DestinationPath $archive -CompressionLevel Optimal
        $artifacts += Get-Item -LiteralPath $archive
    }
    $hashes = foreach ($artifact in $artifacts) {
        "$((Get-FileHash -LiteralPath $artifact.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $($artifact.Name)"
    }
    $checksumFile = Join-Path $scratch 'SHA256SUMS'
    ($hashes -join "`n") + "`n" | Set-Content -LiteralPath $checksumFile -NoNewline -Encoding utf8NoBOM
    foreach ($artifact in $artifacts) { Copy-Item -LiteralPath $artifact.FullName -Destination $destination -Force }
    Copy-Item -LiteralPath $checksumFile -Destination $destination -Force
    $hashes
} finally {
    foreach ($variable in $buildVariables) { [Environment]::SetEnvironmentVariable($variable, $originalEnvironment[$variable]) }
    $resolvedScratch = (Resolve-Path -LiteralPath $scratch).Path
    if ((Split-Path $resolvedScratch -Parent) -ne $destination -or (Split-Path $resolvedScratch -Leaf) -notmatch '^build-[0-9a-f]{32}$') {
        throw "Unexpected temporary build path: $resolvedScratch"
    }
    Remove-Item -LiteralPath $resolvedScratch -Recurse -Force
}
