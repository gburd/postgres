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
      # libxtc main d51d9c8a (2026-11-19), 17 commits after v2.2.0 and not yet
      # tagged.  On top of v2.2.0 it adds the XTC_2.3 receive calls
      # (xtc_recv_mark, xtc_recv_match_since, xtc_recv_match_ex) and fixes
      # three receive paths this tree relies on: a NOCONNECTION DOWN no
      # longer decodes as SIGNAL 4 (432f2f17); supervisors, the registry
      # reaper and xtc_svr_call accept only DOWN and ack messages the
      # library sent (81a553e4); a failed receive copy-out keeps mailbox
      # order (46c6a019).  Re-pin to the release tag that carries XTC_2.3.
      url = "github:gburd/libxtc?rev=d51d9c8a7865e6d306ce8d43543aaf31bdbcee3d";
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
