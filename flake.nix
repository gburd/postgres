{
  description = "PostgreSQL development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    nixpkgs-unstable.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    # libxtc: the async/concurrency runtime PostgreSQL backends run on.
    # Defaults to GitHub; for local development point it at a checkout with
    #   nix develop --override-input libxtc path:$HOME/ws/xtc
    libxtc = {
      # Pinned to the v1.48.1 release tag (560a5bc).  Carries the WRONG-PROC STRAND
      # fixes that are the root cause of our write-path wedge: xtc_proc_wait_fd
      # (v1.48.0) and xtc_proc_sleep (v1.48.1) both took `self` from the
      # __current_proc thread-local, which the coroutine layer restores to a
      # DIFFERENT proc across yields (measured 99.2% wrong under the multi-loop
      # executor).  wait_fd displaced another fiber's fd registration; sleep
      # CANCELLED another fiber's live park timer, stranding a lock holder with no
      # wake source and wedging everything queued behind it.  Also: xtc-stranded now
      # joins fd parks against CQ-overflow state (our request), plus the v1.47.0
      # mailbox-park label, xtc_exit_pid_deadline, and xtc_cfg_ref.
      url = "github:gburd/libxtc?rev=560a5bc5e12e4b677b5c4a21c633d5592d5b9b4c";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
  };

  outputs = {
    self,
    nixpkgs,
    nixpkgs-unstable,
    flake-utils,
    libxtc,
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

        # The built libxtc (headers + libxtc.a + xtc.pc under $out).
        xtc = libxtc.packages.${system}.xtc;

        shellConfig = import ./shell.nix {inherit pkgs pkgs-unstable system xtc;};
      in {
        formatter = pkgs.alejandra;
        devShells = {
          default = shellConfig.devShell;
          gcc = shellConfig.devShell;
          clang = shellConfig.clangDevShell;
          gcc-musl = shellConfig.muslDevShell;
          clang-musl = shellConfig.clangMuslDevShell;
        };

        packages = {
          inherit (shellConfig) gdbConfig flameGraphScript pgbenchScript;
        };

        environment.localBinInPath = true;
      }
    );
}
