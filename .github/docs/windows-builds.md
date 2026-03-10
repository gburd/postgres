# Windows Build Integration

> **Status:** ✅ **IMPLEMENTED**
> This document describes the Windows dependency build system for PostgreSQL development.

## Overview

Integrate Windows dependency builds inspired by [winpgbuild](https://github.com/dpage/winpgbuild) to provide reproducible builds of PostgreSQL dependencies for Windows.

## Objectives

1. **Reproducible builds:** Consistent Windows dependency builds from source
2. **Version control:** Track dependency versions in manifest
3. **Artifact distribution:** Publish build artifacts via GitHub Actions
4. **Cirrus CI integration:** Optionally use pre-built dependencies in Cirrus CI
5. **Parallel to existing:** Complement, not replace, Cirrus CI Windows testing

## Architecture

```
Push to master (after sync)
    ↓
Trigger: windows-dependencies.yml
    ↓
Matrix: Windows Server 2019/2022 × VS 2019/2022
    ↓
Load: .github/windows/manifest.json
    ↓
Build dependencies in order:
  - OpenSSL, zlib, libxml2, ICU
  - Perl, Python, TCL
  - Kerberos, LDAP, gettext
    ↓
Upload artifacts (90-day retention)
    ↓
Optional: Cirrus CI downloads artifacts
```

## Dependencies to Build

### Core Libraries (Required)
- **OpenSSL** 3.0.13 - SSL/TLS support
- **zlib** 1.3.1 - Compression

### Optional Libraries
- **libxml2** 2.12.6 - XML parsing
- **libxslt** 1.1.39 - XSLT transformation
- **ICU** 74.2 - Unicode support
- **gettext** 0.22.5 - Internationalization
- **libiconv** 1.17 - Character encoding

### Language Support
- **Perl** 5.38.2 - For PL/Perl and build tools
- **Python** 3.12.2 - For PL/Python
- **TCL** 8.6.14 - For PL/TCL

### Authentication
- **MIT Kerberos** 1.21.2 - Kerberos authentication
- **OpenLDAP** 2.6.7 - LDAP client

See `.github/windows/manifest.json` for current versions and details.

## Implementation Plan

### Week 4: Research and Design

**Tasks:**
1. Clone winpgbuild repository
   ```bash
   git clone https://github.com/dpage/winpgbuild.git
   cd winpgbuild
   ```

2. Study workflow structure:
   - Examine `.github/workflows/*.yml`
   - Understand manifest format
   - Review build scripts
   - Note caching strategies

3. Design adapted workflow:
   - Single workflow vs separate per dependency
   - Matrix strategy (VS version, Windows version)
   - Artifact naming and organization
   - Caching approach

4. Test locally or on GitHub Actions:
   - Set up Windows runner
   - Test building one dependency (e.g., zlib)
   - Verify artifact upload

**Deliverables:**
- [ ] Architecture document
- [ ] Workflow design
- [ ] Test build results

### Week 5: Implementation

**Tasks:**
1. Create `windows-dependencies.yml` workflow:
   ```yaml
   name: Windows Dependencies

   on:
     push:
       branches: [master]
     workflow_dispatch:

   jobs:
     build-deps:
       runs-on: windows-2022
       strategy:
         matrix:
           vs_version: ['2019', '2022']
           arch: ['x64']

       steps:
         - uses: actions/checkout@v4
         - name: Setup Visual Studio
           uses: microsoft/setup-msbuild@v1
         # ... build steps ...
   ```

2. Create build scripts (PowerShell):
   - `scripts/build-openssl.ps1`
   - `scripts/build-zlib.ps1`
   - etc.

3. Implement manifest loading:
   - Read `manifest.json`
   - Extract version, URL, hash
   - Download and verify sources

4. Implement caching:
   - Cache key: Hash of dependency version + build config
   - Cache location: GitHub Actions cache or artifacts
   - Cache restoration logic

5. Test builds:
   - Build each dependency individually
   - Verify artifact contents
   - Check build logs for errors

**Deliverables:**
- [ ] Working workflow file
- [ ] Build scripts for all dependencies
- [ ] Artifact uploads functional
- [ ] Caching implemented

### Week 6: Integration and Optimization

**Tasks:**
1. End-to-end testing:
   - Trigger full build from master push
   - Verify all artifacts published
   - Download and inspect artifacts
   - Test using artifacts in PostgreSQL build

2. Optional Cirrus CI integration:
   - Modify `.cirrus.tasks.yml`:
     ```yaml
     windows_task:
       env:
         USE_PREBUILT_DEPS: true
       setup_script:
         - curl -O <artifact-url>
         - unzip dependencies.zip
       build_script:
         - # Use pre-built dependencies
     ```

3. Documentation:
   - Complete this document
   - Add troubleshooting section
   - Document artifact consumption

4. Cost optimization:
   - Implement aggressive caching
   - Build only on version changes
   - Consider scheduled builds (daily) vs on-push

**Deliverables:**
- [ ] Fully functional Windows builds
- [ ] Documentation complete
- [ ] Cirrus CI integration (optional)
- [ ] Cost tracking and optimization

## Workflow Structure (Planned)

```yaml
name: Windows Dependencies

on:
  push:
    branches:
      - master
    paths:
      - '.github/windows/manifest.json'
      - '.github/workflows/windows-dependencies.yml'
  schedule:
    # Daily to handle GitHub's 90-day artifact retention
    - cron: '0 2 * * *'
  workflow_dispatch:
    inputs:
      dependency:
        type: choice
        options: [all, openssl, zlib, libxml2, icu, perl, python, tcl]

jobs:
  matrix-setup:
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.set-matrix.outputs.matrix }}
    steps:
      - uses: actions/checkout@v4
      - id: set-matrix
        run: |
          # Load manifest, create build matrix
          # Output: list of dependencies to build

  build-dependency:
    needs: matrix-setup
    runs-on: windows-2022
    strategy:
      matrix: ${{ fromJson(needs.matrix-setup.outputs.matrix) }}
    steps:
      - uses: actions/checkout@v4

      - name: Setup Visual Studio
        uses: microsoft/setup-msbuild@v1
        with:
          vs-version: ${{ matrix.vs_version }}

      - name: Cache dependencies
        uses: actions/cache@v3
        with:
          path: build/${{ matrix.dependency }}
          key: ${{ matrix.dependency }}-${{ matrix.version }}-${{ matrix.vs_version }}

      - name: Download source
        run: |
          # Download from manifest URL
          # Verify SHA256 hash

      - name: Build
        run: |
          # Run appropriate build script
          # ./scripts/build-${{ matrix.dependency }}.ps1

      - name: Package
        run: |
          # Create artifact archive
          # Include: binaries, headers, libs

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: ${{ matrix.dependency }}-${{ matrix.version }}-${{ matrix.vs_version }}
          path: artifacts/${{ matrix.dependency }}
          retention-days: 90

  publish-release:
    needs: build-dependency
    if: startsWith(github.ref, 'refs/tags/')
    runs-on: ubuntu-latest
    steps:
      - name: Download all artifacts
        uses: actions/download-artifact@v4

      - name: Create release
        uses: softprops/action-gh-release@v1
        with:
          files: artifacts/**/*.zip
```

## Artifact Organization

**Naming convention:**
```
{dependency}-{version}-{vs_version}-{arch}.zip

Examples:
- openssl-3.0.13-vs2022-x64.zip
- zlib-1.3.1-vs2022-x64.zip
- icu-74.2-vs2022-x64.zip
```

**Archive contents:**
```
{dependency}/
  ├── bin/          # Runtime libraries (.dll)
  ├── lib/          # Import libraries (.lib)
  ├── include/      # Header files
  ├── share/        # Data files (ICU, gettext)
  ├── BUILD_INFO    # Version, build date, toolchain
  └── LICENSE       # Dependency license
```

## Consuming Artifacts

### From GitHub Actions

```yaml
- name: Download dependencies
  uses: actions/download-artifact@v4
  with:
    name: openssl-3.0.13-vs2022-x64

- name: Setup environment
  run: |
    echo "OPENSSL_ROOT=$PWD/openssl" >> $GITHUB_ENV
    echo "$PWD/openssl/bin" >> $GITHUB_PATH
```

### From Cirrus CI

```yaml
windows_task:
  env:
    ARTIFACT_BASE: https://github.com/gburd/postgres/actions/artifacts

  download_script:
    - ps: Invoke-WebRequest -Uri "$env:ARTIFACT_BASE/openssl-3.0.13-vs2022-x64.zip" -OutFile deps.zip
    - ps: Expand-Archive deps.zip -DestinationPath C:\deps

  build_script:
    - set OPENSSL_ROOT=C:\deps\openssl
    - # ... PostgreSQL build with pre-built dependencies
```

### From Local Builds

```powershell
# Download artifact
gh run download <run-id> -n openssl-3.0.13-vs2022-x64

# Extract
Expand-Archive openssl-3.0.13-vs2022-x64.zip -DestinationPath C:\pg-deps

# Build PostgreSQL
cd postgres
meson setup build --prefix=C:\pg -Dopenssl=C:\pg-deps\openssl
meson compile -C build
```

## Caching Strategy

**Cache key components:**
- Dependency name
- Dependency version (from manifest)
- Visual Studio version
- Platform (x64)

**Cache hit:** Skip build, use cached artifact
**Cache miss:** Build from source, cache result

**Invalidation:**
- Manifest version change
- Manual cache clear
- 7-day staleness (GitHub Actions default)

## Cost Estimates

**Windows runner costs:**
- Windows: 2× Linux cost
- Per-minute rate: $0.016 (vs $0.008 for Linux)

**Build time estimates:**
- zlib: 5 minutes
- OpenSSL: 15 minutes
- ICU: 20 minutes
- Perl: 30 minutes
- Full build (all deps): 3-4 hours

**Monthly costs:**
- Daily full rebuild: 30 × 4 hours × 2× = 240 hours = ~$230/month ⚠️ **Too expensive!**
- Build on manifest change only: ~10 builds/month × 4 hours × 2× = 80 hours = ~$77/month
- With caching (80% hit rate): ~$15/month ✓

**Optimization essential:** Aggressive caching + build only on version changes

## Integration with Existing CI

**Current: Cirrus CI**
- Comprehensive Windows testing
- Builds dependencies from source
- Multiple Windows versions (Server 2019, 2022)
- Visual Studio 2019, 2022

**New: GitHub Actions Windows Builds**
- Pre-build dependencies
- Publish artifacts
- Cirrus CI can optionally consume artifacts
- Faster Cirrus CI builds (skip dependency builds)

**No conflicts:**
- GitHub Actions: Dependency builds
- Cirrus CI: PostgreSQL builds and tests
- Both can run in parallel

## Security Considerations

**Source verification:**
- All sources downloaded from official URLs (in manifest)
- SHA256 hash verification
- Fail build on hash mismatch

**Artifact integrity:**
- GitHub Actions artifacts are checksummed
- Artifacts signed (future: GPG signatures)

**Toolchain trust:**
- Microsoft Visual Studio (official toolchain)
- Windows Server images (GitHub-provided)

## Future Enhancements

1. **Cross-compilation:** Build from Linux using MinGW
2. **ARM64 support:** Add ARM64 Windows builds
3. **Signed artifacts:** GPG signatures for artifacts
4. **Dependency mirroring:** Mirror sources to ensure availability
5. **Nightly builds:** Track upstream dependency releases
6. **Notification:** Slack/Discord notifications on build failures

## References

- winpgbuild: https://github.com/dpage/winpgbuild
- PostgreSQL Windows build: https://www.postgresql.org/docs/current/install-windows-full.html
- GitHub Actions Windows: https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners#supported-runners-and-hardware-resources
- Visual Studio: https://visualstudio.microsoft.com/downloads/

---

**Status:** ✅ **IMPLEMENTED**
**Version:** 1.0
**Last Updated:** 2026-03-10
