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
      # v2.2.0 (tag object 6d45d849, commit 2e2e10b0, 2026-11-18): the first
      # release of the 2.2 line.  SONAME stays libxtc.so.2; the 2.1.2 ABI is
      # kept at XTC_2.0 and the new API is in XTC_2.2.  It contains the
      # stale-readiness generation-token fix (the fd107 XTC_E_INVAL root
      # cause), the DOWN-classification fix (a contained fault reports
      # XTC_DOWN_KIND_SIGNAL whatever code the fiber passed to xtc_exit_self;
      # a kill reports XTC_DOWN_KIND_KILLED), xtc_sup_opts_t.on_child_down,
      # and the xtc_reg value API (xtc_reg_put / xtc_reg_visit).  2.2 renames
      # part of the API with the old names kept until 3.0 (libxtc
      # docs/deprecations.md); this tree uses the new names.
      url = "github:gburd/libxtc?rev=2e2e10b00a9a2c4d66892153498bf5a7c067f44b";
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
