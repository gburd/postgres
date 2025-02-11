{
  description = "PostgreSQL";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ...}:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          config = {
            allowUnfree = true;
          };
          inherit system;
        };
      in {
        devShell = with pkgs; mkShell {
          buildInputs = [
            coreutils
            shellcheck
            ripgrep
            autoconf
            libtool
            valgrind

            cmake
            meson
            ninja
            #(perl.withPackages(ps: [ ps.PerlTidy ])) # 20230309

            #clang
            clang-tools
            lldb

            gdb
            gcc

            bison
            flex
            readline
            zlib
            icu
          ];
          shellHook = ''
            export HISTFILE=.history
          '';
        };
      }
    );
}
