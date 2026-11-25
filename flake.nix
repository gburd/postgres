{
  description = "PostgreSQL development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    nixpkgs-unstable.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    # libumem: LD_PRELOAD malloc, the umem/umemctl CLI, and GDB/LLDB helpers.
    # Track the latest tagged release that BUILDS.  Bump the ref to a newer tag
    # and run `nix flake update libumem`; do NOT point this at a branch, so the
    # dev shell always builds a released version rather than an arbitrary
    # master commit.
    #
    # Pinned to v4.2.1, the latest tag whose own flake.nix `version` matches
    # its configure.ac AC_INIT.  v4.2.2..v4.3.2 left the flake version at
    # "4.2.1" while bumping configure.ac, so libumem's own build assertion
    # (flake.nix version != configure.ac AC_INIT) fails.  Bump this ref once
    # upstream syncs the two version strings again.
    #
    # Follow nixpkgs-unstable, the same pin the dev shell's compiler and
    # libraries use, so libumem links the same glibc the dev shell does.
    # Mixing glibc versions here produced a GLIBC_PRIVATE symbol-lookup
    # failure when LD_PRELOAD'ing libumem into freshly built binaries.
    libumem = {
      url = "git+https://codeberg.org/gregburd/libumem?ref=refs/tags/v4.2.1";
      inputs.nixpkgs.follows = "nixpkgs-unstable";
    };
  };

  outputs = {
    self,
    nixpkgs,
    nixpkgs-unstable,
    flake-utils,
    libumem,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
        pkgs-unstable = import nixpkgs-unstable {
          inherit system;
          config.allowUnfree = true;
        };

        libumemPkg = libumem.packages.${system}.libumem;

        shellConfig = import ./shell.nix {
          inherit pkgs pkgs-unstable system libumemPkg;
        };
      in {
        formatter = pkgs.alejandra;
        devShells = {
          default = shellConfig.devShell;
          gcc = shellConfig.devShell;
          clang = shellConfig.clangDevShell;
        };

        packages = {
          inherit (shellConfig) gdbConfig flameGraphScript pgbenchScript;
          libumem = libumemPkg;
        };

        environment.localBinInPath = true;
      }
    );
}
