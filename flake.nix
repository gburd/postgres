{
  description = "PostgreSQL development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    nixpkgs-unstable.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    # libumem: LD_PRELOAD malloc, the umem/umemctl CLI, and GDB/LLDB helpers.
    # Track the latest tagged release from the repo.  Bump the ref to the new
    # tag and run `nix flake update libumem` to pick up a newer release; do NOT
    # point this at a branch, so the dev shell always builds a released
    # version rather than an arbitrary master commit.
    #
    # Follow the stable nixpkgs so libumem links the same glibc the dev
    # shell's base tools (bash, coreutils) use; following unstable produced a
    # glibc 2.42-vs-2.40 GLIBC_ABI mismatch when LD_PRELOAD'ing into them.
    libumem = {
      url = "git+https://codeberg.org/gregburd/libumem?ref=refs/tags/v4.1.0";
      inputs.nixpkgs.follows = "nixpkgs";
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
