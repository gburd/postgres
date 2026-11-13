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
      # DEV PIN (2026-11-13): libxtc main @ 8a7e9c06 -- the 2.2 development line
      # (symbol node XTC_2.2), pushed as origin/main.  It supersedes the earlier
      # 6dd001e0 pin and contains, on top of the stale-readiness gen-token fix
      # (the fd107 XTC_E_INVAL root cause, released as v2.1.2):
      #   - the DOWN-classification fix: a contained fault is always reported as
      #     XTC_DOWN_KIND_SIGNAL regardless of the exit code the fiber passed to
      #     xtc_exit_self, plus the new XTC_DOWN_KIND_KILLED (=5) value.  v2.1.2
      #     still MISREPORTS our backends' "fault inside the recovery frame ->
      #     xtc_exit_self(sig)" pattern as KIND_EXIT, which silently skipped our
      #     genuine-crash fail-stop; this commit fixes that (reply
      #     /tmp/libxtc-reply-pgreq-2026-11-12.md section 1).
      #   - the xtc_orc per-child DOWN hook (xtc_sup_opts_t.on_child_down).
      #   - the xtc_reg value API (xtc_reg_put / xtc_reg_visit).
      # This is the 2.2 DEV line, not an LTS tag (no v2.2.x tag exists yet).
      # Re-pin to the v2.2.x release tag once cut.  No PG runtime validation has
      # been done against this pin yet (EC2 build pending this session).
      url = "github:gburd/libxtc?rev=8a7e9c06bcda2ea9241dbc5cacee48fde7fb2620";
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
