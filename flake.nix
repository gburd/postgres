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
      # DEV PIN (2026-11-04): fix/stale-readiness-dispatch @ 6dd001e0 -- carries
      # the stale-readiness gen-token fix (the fd107 XTC_E_INVAL root cause) AND
      # the OTP headers (xtc_svr/xtc_orc/xtc_reg/xtc_xproc) and the TLS
      # transport/SNI hooks (xtc_tls_create_transport, xtc_tls_ctx_set_sni_cb)
      # this branch's libxtc-fusion gap-closing work consumes.  THIS COMMIT IS
      # UNRELEASED/UNPUSHED (local only): before any trusted validation, re-pin
      # to the released v2.1.2 tag once the libxtc team cuts it, and confirm the
      # tree matches.  No PG runtime validation has been done against this pin.
      url = "github:gburd/libxtc?rev=6dd001e02cd52d64dcde0be6df43600b7e89b696";
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
