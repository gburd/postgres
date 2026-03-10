# Using Windows Dependencies

Quick guide for consuming the Windows dependencies built by GitHub Actions.

## Quick Start

### Option 1: Using GitHub CLI (Recommended)

```powershell
# Install gh CLI if needed
# https://cli.github.com/

# Download latest successful build
gh run list --repo gburd/postgres --workflow windows-dependencies.yml --status success --limit 1

# Get the run ID from above, then download
gh run download <RUN_ID> -n postgresql-deps-bundle-win64

# Extract and set environment
$env:PATH = "$(Get-Location)\postgresql-deps-bundle-win64\bin;$env:PATH"
$env:OPENSSL_ROOT_DIR = "$(Get-Location)\postgresql-deps-bundle-win64"
```

### Option 2: Using Helper Script

```powershell
# Download our helper script
curl -O https://raw.githubusercontent.com/gburd/postgres/master/.github/scripts/windows/download-deps.ps1

# Run it (downloads latest)
.\download-deps.ps1 -Latest -OutputPath C:\pg-deps

# Add to PATH
$env:PATH = "C:\pg-deps\bin;$env:PATH"
```

### Option 3: Manual Download

1. Go to: https://github.com/gburd/postgres/actions
2. Click: **"Build Windows Dependencies"**
3. Click on a successful run (green ✓)
4. Scroll down to **Artifacts**
5. Download: **postgresql-deps-bundle-win64**
6. Extract to `C:\pg-deps`

## Using with PostgreSQL Build

### Meson Build

```powershell
# Set dependency paths
$env:PATH = "C:\pg-deps\bin;$env:PATH"
$env:OPENSSL_ROOT_DIR = "C:\pg-deps"
$env:ZLIB_ROOT = "C:\pg-deps"

# Configure PostgreSQL
meson setup build `
  --prefix=C:\pgsql `
  -Dssl=openssl `
  -Dzlib=enabled `
  -Dlibxml=enabled

# Build
meson compile -C build

# Install
meson install -C build
```

### MSVC Build (traditional)

```powershell
cd src\tools\msvc

# Edit config.pl - add dependency paths
# $config->{openssl} = 'C:\pg-deps';
# $config->{zlib} = 'C:\pg-deps';
# $config->{libxml2} = 'C:\pg-deps';

# Build
build.bat

# Install
install.bat C:\pgsql
```

## Environment Variables Reference

```powershell
# Required for most builds
$env:PATH = "C:\pg-deps\bin;$env:PATH"

# OpenSSL
$env:OPENSSL_ROOT_DIR = "C:\pg-deps"
$env:OPENSSL_INCLUDE_DIR = "C:\pg-deps\include"
$env:OPENSSL_LIB_DIR = "C:\pg-deps\lib"

# zlib
$env:ZLIB_ROOT = "C:\pg-deps"
$env:ZLIB_INCLUDE_DIR = "C:\pg-deps\include"
$env:ZLIB_LIBRARY = "C:\pg-deps\lib\zlib.lib"

# libxml2
$env:LIBXML2_ROOT = "C:\pg-deps"
$env:LIBXML2_INCLUDE_DIR = "C:\pg-deps\include\libxml2"
$env:LIBXML2_LIBRARIES = "C:\pg-deps\lib\libxml2.lib"

# ICU (if built)
$env:ICU_ROOT = "C:\pg-deps"
```

## Checking What's Installed

```powershell
# Check manifest
Get-Content C:\pg-deps\BUNDLE_MANIFEST.json | ConvertFrom-Json | ConvertTo-Json -Depth 10

# List all DLLs
Get-ChildItem C:\pg-deps\bin\*.dll

# List all libraries
Get-ChildItem C:\pg-deps\lib\*.lib

# Check OpenSSL version
& C:\pg-deps\bin\openssl.exe version
```

## Troubleshooting

### Missing DLLs at Runtime

**Problem:** `openssl.dll not found` or similar

**Solution:** Add dependencies to PATH:
```powershell
$env:PATH = "C:\pg-deps\bin;$env:PATH"
```

Or copy DLLs to your PostgreSQL bin directory:
```powershell
Copy-Item C:\pg-deps\bin\*.dll C:\pgsql\bin\
```

### Build Can't Find Headers

**Problem:** `openssl/ssl.h: No such file or directory`

**Solution:** Set include directories:
```powershell
$env:INCLUDE = "C:\pg-deps\include;$env:INCLUDE"
```

Or pass to compiler:
```
/IC:\pg-deps\include
```

### Linker Can't Find Libraries

**Problem:** `LINK : fatal error LNK1181: cannot open input file 'libssl.lib'`

**Solution:** Set library directories:
```powershell
$env:LIB = "C:\pg-deps\lib;$env:LIB"
```

Or pass to linker:
```
/LIBPATH:C:\pg-deps\lib
```

### Version Conflicts

**Problem:** Multiple OpenSSL versions on system

**Solution:** Ensure our version comes first in PATH:
```powershell
# Prepend our path
$env:PATH = "C:\pg-deps\bin;" + $env:PATH

# Verify
(Get-Command openssl).Source
# Should show: C:\pg-deps\bin\openssl.exe
```

## CI/CD Integration

### GitHub Actions

```yaml
- name: Download Dependencies
  run: |
    gh run download <RUN_ID> -n postgresql-deps-bundle-win64
    Expand-Archive postgresql-deps-bundle-win64.zip -DestinationPath C:\pg-deps

- name: Setup Environment
  run: |
    echo "C:\pg-deps\bin" >> $env:GITHUB_PATH
    echo "OPENSSL_ROOT_DIR=C:\pg-deps" >> $env:GITHUB_ENV
```

### Cirrus CI

```yaml
windows_task:
  env:
    DEPS_URL: https://github.com/gburd/postgres/actions/artifacts/...

  download_script:
    - ps: |
        gh run download $env:RUN_ID -n postgresql-deps-bundle-win64
        Expand-Archive postgresql-deps-bundle-win64.zip -DestinationPath C:\pg-deps

  env_script:
    - ps: |
        $env:PATH = "C:\pg-deps\bin;$env:PATH"
        $env:OPENSSL_ROOT_DIR = "C:\pg-deps"
```

## Building Your Own

If you need different versions or configurations:

```powershell
# Fork the repository
# Edit .github/windows/manifest.json to update versions

# Trigger build manually
gh workflow run windows-dependencies.yml --repo your-username/postgres

# Or trigger specific dependency
gh workflow run windows-dependencies.yml -f dependency=openssl
```

## Artifact Retention

- **Retention:** 90 days
- **Refresh:** Automatically weekly (Sundays 4 AM UTC)
- **On-demand:** Trigger manual build anytime via Actions tab

If artifacts expire:
1. Go to: Actions → Build Windows Dependencies
2. Click: "Run workflow"
3. Select: "all" (or specific dependency)
4. Click: "Run workflow"

## Support

**Issues:** https://github.com/gburd/postgres/issues

**Documentation:**
- Build system: `.github/docs/windows-builds.md`
- Workflow: `.github/workflows/windows-dependencies.yml`
- Manifest: `.github/windows/manifest.json`
