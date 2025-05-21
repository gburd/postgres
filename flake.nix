# To get the default shell (latest GCC, no IKOS): nix develop .
# To get a refreshed default shell: nix develop .#gcc.default --refresh
# For the latest GCC: nix develop .#gcc or nix develop .#gcc.latest
# For GCC 13 without IKOS: nix develop .#gcc.13
# For GCC 13 with IKOS: nix develop .#gcc.13.with-ikos
# For the latest Clang: nix develop .#clang or nix develop .#clang.latest
# For Clang 17 without IKOS: nix develop .#clang.17.no-ikos
# For Clang 17 with IKOS: nix develop .#clang.17.with-ikos

{
  description = "PostgreSQL development environment with selectable compiler.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        # Define an overlay to pin PerlTidy to version 20230309
        perltidy-overlay = final: prev: {
          PerlTidy = prev.perlPackages.buildPerlPackage rec {
            # You MUST provide the correct sha256 for the source of this specific version.
            # To find this, you'd typically:
            # 1. Go to CPAN: https://metacpan.org/release/Perl-Tidy
            # 2. Find the release for 20230309.
            # 3. Download the tar.gz file.
            # 4. Use `nix-prefetch-url --type sha256 <URL_TO_TARBALL>`
            #    or `nix-hash --flat --base32 --type sha256 <PATH_TO_DOWNLOADED_TARBALL>`
            #    (e.g., if you downloaded Perl-Tidy-20230309.tar.gz)
            #    You'll need `nix-prefetch-url https://cpan.metacpan.org/authors/id/P/PH/PHRED/Perl-Tidy-20230309.tar.gz`
            #    to get the sha256.
            pname = "Perl-Tidy";
            version = "20230309";
            src = final.fetchurl {
              url = "https://cpan.metacpan.org/authors/id/S/SH/SHANCOCK/Perl-Tidy-20230309.tar.gz";
              sha256 = "sha256-4ilJogjGGNZxoYxYKbRRq76doNos3deP2/ywNsc2HBg=";
            };
          };
        };

        pkgs = import nixpkgs {
          inherit system;
          config = {
            allowUnfree = true;
          };
          overlays = [ perltidy-overlay ];
        };

        # Custom IKOS package definition
        ikos = pkgs.stdenv.mkDerivation rec {
          pname = "ikos";
          version = "3.5"; # Or "unstable-${src.rev}" if you fetch a mutable branch
          # Fetch IKOS source directly within the derivation
          src = pkgs.fetchFromGitHub {
            owner = "NASA-SW-VnV";
            repo = "ikos";
            rev = "1d98c65d282554ffb6997dba67b0f8e41e22e169"; # This is the specific commit hash for v3.5
            sha256 = "sha256-n6V04iF+QZ4+4x32s5Q6m1C8g7B9e0k3j2f1d8m9o0o="; # This is the content hash for v3.5
          };
          buildInputs = with pkgs; with pkgs.python3Packages; [
            llvmPackages.llvm
            llvmPackages.clang
            zlib
            sqlite
            boost
            gmp
            tbb
            python3
            pygments
          ];

          nativeBuildInputs = with pkgs; [
            cmake
            ninja
            pkg-config
          ];

          cmakeFlags = [
            "-DCMAKE_INSTALL_PREFIX=${placeholder "out"}"
            "-DLLVM_CONFIG_EXECUTABLE=${pkgs.llvmPackages.llvm}/bin/llvm-config"
          ];
        };

        # Define common dependencies for PostgreSQL
        commonPgBuildDeps = with pkgs; [
          binutils
          gnumake
          meson
          ninja
          pkg-config
          autoconf
          libtool
          git

          bison # Yacc-compatible parser generator (for SQL parser)
          flex # Lexical analyzer generator
          #perl # Various build scripts and PL/Perl
          (perl.withPackages(ps: [ ps.IPCRun ]))
          # (perl.withPackages(ps: [ ps.PerlTidy ])) # 20230309
          docbook_xml_dtd_45 # For documentation generation
          docbook-xsl-nons # For documentation generation
          libxslt # For processing XSLT stylesheets (DocBook)
          libxml2 # For XML support (PostgreSQL features, also a DocBook dep)

          readline # For `psql` command-line editing
          zlib # For compression/decompression
          openssl # For SSL/TLS support
          icu # International Components for Unicode (for ICU collations)
          lz4 # For LZ4 compression (PostgreSQL supports this, from PG 14+)
          zstd # For ZSTD compression (PostgreSQL supports this, from PG 15+)
          libuuid # For UUID support (e2fs provides this)
          libkrb5 # Kerberos support (GSSAPI)
          linux-pam # For PAM authentication support (Linux-specific)
          libxcrypt # Used by PlPerl
          numactl # For Non-uniform memory access features
          llvmPackages.llvm # For bit code
          openldap # For LDAP API/interface

          python3 # For PL/Python procedural language and some build/test scripts
          tcl # For PL/Tcl procedural language
          curl
          #(pkgs.curl.override { openssl = pkgs.openssl; }) # For libpq if it's built with libcurl support
          liburing # For io_uring support (newer kernel features)
          libselinux # For SELinux support

          coreutils # Provides essential Unix utilities
          shellcheck # Static analysis tool for shell scripts
          ripgrep # Fast code search utility
          valgrind # Memory debugging and profiling tool
          fop # Formatter for XML/XSL-FO

          # Debuggers (gdb is for GCC/Clang, lldb is for Clang)
          gdb
          lldb

          glibc
        ];

        # Define PostgreSQL source and build directories as Nix variables
        pgSourceDir = "$HOME/ws/postgresql";
        pgBuildDir = "$HOME/ws/postgresql/build";
        pgInstallDir = "$HOME/ws/postgresql/test-db";

        # Function to generate common shell hook
        generateCommonShellHook = {
          compilerName,
          compilerPath,
          ikosEnabled ? false,
          ikosPath ? null,
          ccacheEnabled ? false,
          clangTidyEnabled ? false,
          cppcheckEnabled ? false
        }: ''
          export HISTFILE=.history
          export HISTSIZE=1000000
          export HISTFILESIZE=1000000

          # --- Ccache Setup ---
          ${pkgs.lib.optionalString ccacheEnabled ''
            echo "ccache is ENABLED. Compiler commands will be cached."
            export PATH=${pkgs.ccache}/bin:$PATH # Prepend ccache to PATH
            export CCACHE_COMPILERCHECK=content # More robust checking
            export CCACHE_DIR=$HOME/.ccache_pg_dev # Custom cache directory
            mkdir -p "$CCACHE_DIR"
            echo "Ccache directory: $CCACHE_DIR"
          ''}

          # Set primary compilers (potentially wrapped by ccache due to PATH)
          export CC="${compilerPath}/bin/${compilerName}"
          export CXX="${compilerPath}/bin/${compilerName}++"
          export LD="${compilerPath}/bin/ld"

          ${pkgs.lib.optionalString (compilerName == "clang") "export LDFLAGS=\"-fuse-ld=lld \$LDFLAGS\""}

          echo "Entering PostgreSQL development environment with ${compilerName}."
          echo "CC: $CC"
          echo "CXX: $CXX"

          # --- Static Analysis Tools Integration ---
          ${pkgs.lib.optionalString ikosEnabled ''
            echo "IKOS static analyzer is ENABLED."
            export PATH=${ikosPath}/bin:$PATH # Prepend IKOS to PATH for ikos-scan to work
            echo "To analyze a project with IKOS, you might use 'ikos-scan'."
            echo "Example: ikos-scan -- ${compilerName} -c your_file.c"
          ''}

          ${pkgs.lib.optionalString (clangTidyEnabled && compilerName == "clang") ''
            echo "Clang-Tidy is ENABLED."
            export PATH=${pkgs.clang-tools}/bin:$PATH # clang-tidy is in clang-tools
            echo "To run clang-tidy on a file: clang-tidy your_file.c -- -Isrc -Ipath/to/includes"
            echo "Consider integrating clang-tidy with your build system (e.g., via CMake/Meson hooks or cflags)."
          ''}

          ${pkgs.lib.optionalString cppcheckEnabled ''
            echo "Cppcheck static analyzer is ENABLED."
            export PATH=${pkgs.cppcheck}/bin:$PATH
            echo "To run Cppcheck: cppcheck --enable=all --inconclusive --std=c11 your_project_root/"
            echo "Make sure to adjust flags for PostgreSQL's specific C standard and includes."
          ''}

          export PERL_CORE_DIR=$(find ${pkgs.perl} -maxdepth 5 -path "*/CORE" -type d)

          # --- PostgreSQL Build/Test Aliases ---
          export PG_SOURCE_DIR="${pgSourceDir}"
          export PG_BUILD_DIR="${pgBuildDir}"
          export PG_INSTALL_DIR="${pgInstallDir}"

          alias pg-setup='
            if [ -z "$PERL_CORE_DIR" ]; then
              echo "Error: Could not find perl CORE directory in ${pkgs.perl}. Check your Perl installation." >&2
              return 1
            fi

            env CFLAGS="-I$PERL_CORE_DIR $CFLAGS" \
                LDFLAGS="-L$PERL_CORE_DIR -lperl $LDFLAGS" \
            meson setup --reconfigure \
              -Dlz4=enabled \
              -Dplperl=enabled \
              -Dplpython=enabled \
              -Dpltcl=enabled \
              -Dlibxml=enabled \
              -Duuid=e2fs \
              -Dlibxslt=enabled \
              -Ddebug=true \
              -Dcassert=true \
              -Dtap_tests=enabled \
              -Ddocs_pdf=enabled \
              -Ddocs_html_style=website \
              -Dssl=openssl \
              -Dldap=disabled \
              --werror \
              "$PG_BUILD_DIR" \
              "$PG_SOURCE_DIR" \
              || { echo "Meson configure failed!"; return 1; }'
          alias pg-build='ninja -C "$PG_BUILD_DIR" || { echo "Ninja build failed!"; return 1; }'
          alias pg-install='
            echo "Installing PostgreSQL to system..."
            meson install -C "$PG_BUILD_DIR" --prefix "$PG_INSTALL_DIR" || { echo "Installation failed!"; return 1; }
            echo "Installation completed."
          '
          alias pg-docs='ninja -C "$PG_BUILD_DIR docs" || { echo "Ninja documentation build failed!"; return 1; }'
          alias pg-check='ninja -C build check'
          alias pg-test='meson test -C "$PG_BUILD_DIR"'
          alias pg-list-tests='meson test -C "$PG_BUILD_DIR" --list'
          alias pg-run-tests='meson test -C "$PG_BUILD_DIR" "$@"'
          alias pg-clean='ninja -C "$PG_BUILD_DIR" clean'
          alias pg-maintainer-clean='(cd "$PG_SOURCE_DIR" && ./configure --without-icu > /dev/null 2>&1 && make maintainer-clean > /dev/null 2>&1) || true'

          echo "To configure PostgreSQL with Meson, run: pg-setup"
          echo "To configure PostgreSQL with Autoconf, run: pg-configure"
          echo "To compile PostgreSQL, run: pg-build"
          echo "To run tests (after building): pg-check"
          echo "To maintainer clean: pg-maintainer-clean"
        '';

        # Helper to get the latest version from a range
        latestGCCVersion = pkgs.lib.last (pkgs.lib.range 11 14);
        latestClangVersion = pkgs.lib.last (pkgs.lib.range 15 20);
        stringRange = start: end: pkgs.lib.map toString (pkgs.lib.range start end);

        # Define common optional tools attribute set for reuse
        # This structure allows selecting any combination of tools
        # It's a function that takes compiler info and returns the set of shell variants
        optionalTools = { compilerName, compilerPath }: {
          none = pkgs.mkShell {
            name = "postgresql-${compilerName}-shell";
            buildInputs = commonPgBuildDeps ++ [ compilerPath ];
            shellHook = generateCommonShellHook {
              inherit compilerName compilerPath;
            };
          };
          with-ccache = pkgs.mkShell {
            name = "postgresql-${compilerName}-ccache-shell";
            buildInputs = commonPgBuildDeps ++ [ compilerPath pkgs.ccache ];
            shellHook = generateCommonShellHook {
              inherit compilerName compilerPath;
              ccacheEnabled = true;
            };
          };
          with-ikos = pkgs.mkShell {
            name = "postgresql-${compilerName}-ikos-shell";
            buildInputs = commonPgBuildDeps ++ [ compilerPath ikos ];
            shellHook = generateCommonShellHook {
              inherit compilerName compilerPath ikos;
              ikosEnabled = true;
            };
          };
          with-clang-tidy = pkgs.mkShell {
            name = "postgresql-${compilerName}-clang-tidy-shell";
            buildInputs = commonPgBuildDeps ++ [ compilerPath pkgs.clang-tools ]; # clang-tidy is in clang-tools
            shellHook = generateCommonShellHook {
              inherit compilerName compilerPath;
              clangTidyEnabled = true;
            };
          };
          with-cppcheck = pkgs.mkShell {
            name = "postgresql-${compilerName}-cppcheck-shell";
            buildInputs = commonPgBuildDeps ++ [ compilerPath pkgs.cppcheck ];
            shellHook = generateCommonShellHook {
              inherit compilerName compilerPath;
              cppcheckEnabled = true;
            };
          };
          with-ccache-clang-tidy = pkgs.mkShell {
            name = "postgresql-${compilerName}-ccache-clang-tidy-shell";
            buildInputs = commonPgBuildDeps ++ [ compilerPath pkgs.ccache pkgs.clang-tools ];
            shellHook = generateCommonShellHook {
              inherit compilerName compilerPath;
              ccacheEnabled = true;
              clangTidyEnabled = true;
            };
          };
        };


        # GCC-based shells
        # Use stringRange to get a list of strings like [ "11" "12" "13" ]
        gccShells = pkgs.lib.genAttrs (stringRange 11 14) (versionString: # versionString will now be "11", "12", etc.
          let
            # No need for toString here, as versionString is already a string
            compilerName = "gcc";
            compilerPath = pkgs."gcc${versionString}"; # Use versionString directly
            shellsForVersion = optionalTools { inherit compilerName compilerPath; };
          in
          shellsForVersion // { # Merge the generated shells with any version-specific aliases
            latest = shellsForVersion.none; # Alias for the latest GCC version, defaults to 'none' for tool options
          }
        );


        # Clang-based shells
        # Use stringRange to get a list of strings like [ "15" "16" "17" ]
        clangShells = pkgs.lib.genAttrs (stringRange 15 20) (versionString: # versionString will now be "15", "16", etc.
          let
            # No need for toString here, as versionString is already a string
            compilerName = "clang";
            compilerPath = pkgs."clang_${versionString}"; # Use versionString directly
            shellsForVersion = optionalTools { inherit compilerName compilerPath; };
          in
          shellsForVersion // { # Merge the generated shells with any version-specific aliases
            latest = shellsForVersion.none; # Alias for the latest Clang version, defaults to 'none' for tool options
          }
        );

      in {
        # Export the composed devShells
        devShells.gcc = gccShells // {
          default = gccShells.${toString latestGCCVersion}.none;
        };

        devShells.clang = clangShells // {
          default = clangShells.${toString latestClangVersion}.none;
        };

        devShells.default = self.devShells.${system}.gcc.default;

      }
    );
}
