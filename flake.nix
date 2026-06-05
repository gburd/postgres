{
  description = "PostgreSQL development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    nixpkgs-unstable.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    # Lime parser generator -- replaces bison/flex for this branch.
    # Built from source by Nix; provides the `lime` binary and its
    # runtime extension library.  Pinned via flake.lock; override with
    #   nix develop --override-input lime path:/path/to/lime
    # for local development against an unpublished branch.
    lime = {
      url = "git+https://codeberg.org/gregburd/lime.git?ref=refs/tags/v1.5.3";
      inputs.nixpkgs.follows = "nixpkgs-unstable";
      inputs.flake-utils.follows = "flake-utils";
    };
  };

  outputs = {
    self,
    nixpkgs,
    nixpkgs-unstable,
    flake-utils,
    lime,
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

        limePkg = let
          base = lime.packages.${system}.default;
        in
          base.overrideAttrs (old: {
            # Upstream Lime doesn't install its parser-driver template
            # (limpar.c) as part of `meson install`, and lime.c searches
            # for the template under the Lemon-era name "lempar.c"
            # (lime.c:4650).  Install the file next to the binary under
            # that name so `lime foo.lime` finds it without -T<path>.
            # Both issues should be fixed upstream; tracked in
            # Lime-Requests.txt.
            postInstall = (old.postInstall or "") + ''
              install -Dm0644 ${lime}/limpar.c $out/bin/lempar.c
            '';
          });

        shellConfig = import ./shell.nix {
          inherit pkgs pkgs-unstable system limePkg;
        };
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
          lime = limePkg;
        };

        environment.localBinInPath = true;
      }
    );
}
