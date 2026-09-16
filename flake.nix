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
      # Pinned to the v1.47.0 release tag (76d1f73).  Carries the v1.44.1 (2c851a5)
      # cross-loop aio lost-wake fix, the v1.44.0 xtc_orc atomic spawn+monitor, the
      # v1.45.0 xtc_tail dial9 microscope, and four fixes made in response to our
      # reports: park_reason now names the MAILBOX park (a sourceless park was
      # indistinguishable from a lost wake -- it manufactured our wedge evidence),
      # xtc-gdb/lldb fail LOUD instead of printing an empty census when libxtc has
      # no debug info, xtc_exit_pid_deadline + mask state in xtc_proc_info, and
      # xtc_cfg_ref pointer-stable read handles.
      url = "github:gburd/libxtc?rev=76d1f736d40a055c5a0d765b69d02eaede4a18b7";
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
