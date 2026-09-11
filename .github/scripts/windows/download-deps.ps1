# Download and extract PostgreSQL Windows dependencies from GitHub Actions artifacts
#
# Usage:
#   .\download-deps.ps1 -RunId <run-id> -Token <github-token> -OutputPath C:\pg-deps
#
# Or use gh CLI:
#   gh run download <run-id> -n postgresql-deps-bundle-win64

param(
    [Parameter(Mandatory=$false)]
    [string]$RunId,

    [Parameter(Mandatory=$false)]
    [string]$Token = $env:GITHUB_TOKEN,

    [Parameter(Mandatory=$false)]
    [string]$OutputPath = "C:\pg-deps",

    [Parameter(Mandatory=$false)]
    [string]$Repository = "gburd/postgres",

    [Parameter(Mandatory=$false)]
    [switch]$Latest
)

$ErrorActionPreference = "Stop"

Write-Host "PostgreSQL Windows Dependencies Downloader" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Check for gh CLI
$ghAvailable = Get-Command gh -ErrorAction SilentlyContinue

if ($ghAvailable) {
    Write-Host "Using GitHub CLI (gh)..." -ForegroundColor Green

    if ($Latest) {
        Write-Host "Finding latest successful build..." -ForegroundColor Yellow
        $runs = gh run list --repo $Repository --workflow windows-dependencies.yml --status success --limit 1 --json databaseId | ConvertFrom-Json

        if ($runs.Count -eq 0) {
            Write-Host "No successful runs found" -ForegroundColor Red
            exit 1
        }

        $RunId = $runs[0].databaseId
        Write-Host "Latest run ID: $RunId" -ForegroundColor Green
    }

    if (-not $RunId) {
        Write-Host "ERROR: RunId required when not using -Latest" -ForegroundColor Red
        exit 1
    }

    Write-Host "Downloading artifacts from run $RunId..." -ForegroundColor Yellow

    # Create temp directory
    $tempDir = New-Item -ItemType Directory -Force -Path "$env:TEMP\pg-deps-download-$(Get-Date -Format 'yyyyMMddHHmmss')"

    try {
        Push-Location $tempDir

        # Download bundle
        gh run download $RunId --repo $Repository -n postgresql-deps-bundle-win64

        # Extract to output path
        Write-Host "Extracting to $OutputPath..." -ForegroundColor Yellow
        New-Item -ItemType Directory -Force -Path $OutputPath | Out-Null

        Copy-Item -Path "postgresql-deps-bundle-win64\*" -Destination $OutputPath -Recurse -Force

        Write-Host ""
        Write-Host "Success! Dependencies installed to: $OutputPath" -ForegroundColor Green
        Write-Host ""

        # Show manifest
        if (Test-Path "$OutputPath\BUNDLE_MANIFEST.json") {
            $manifest = Get-Content "$OutputPath\BUNDLE_MANIFEST.json" | ConvertFrom-Json
            Write-Host "Dependencies:" -ForegroundColor Cyan
            foreach ($dep in $manifest.dependencies) {
                Write-Host "  - $($dep.name) $($dep.version)" -ForegroundColor White
            }
            Write-Host ""
        }

        # Instructions
        Write-Host "To use these dependencies, add to your PATH:" -ForegroundColor Yellow
        Write-Host '  $env:PATH = "' + $OutputPath + '\bin;$env:PATH"' -ForegroundColor White
        Write-Host ""
        Write-Host "Or set environment variables:" -ForegroundColor Yellow
        Write-Host '  $env:OPENSSL_ROOT_DIR = "' + $OutputPath + '"' -ForegroundColor White
        Write-Host '  $env:ZLIB_ROOT = "' + $OutputPath + '"' -ForegroundColor White
        Write-Host ""

    } finally {
        Pop-Location
        Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
    }

} else {
    Write-Host "GitHub CLI (gh) not found" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install gh CLI: https://cli.github.com/" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Or download manually:" -ForegroundColor Yellow
    Write-Host "  1. Go to: https://github.com/$Repository/actions" -ForegroundColor White
    Write-Host "  2. Click on 'Build Windows Dependencies' workflow" -ForegroundColor White
    Write-Host "  3. Click on a successful run" -ForegroundColor White
    Write-Host "  4. Download 'postgresql-deps-bundle-win64' artifact" -ForegroundColor White
    Write-Host "  5. Extract to $OutputPath" -ForegroundColor White
    exit 1
}
