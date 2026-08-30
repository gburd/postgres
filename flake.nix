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
      # Pinned to the v1.40.3 release rev (not branch HEAD: `nix flake update`
      # would otherwise pull post-tag commits).
      url = "github:gburd/libxtc?rev=d0adff7ed32fa3a5fc4fdeb8b0f90630ed7f84eb";
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
