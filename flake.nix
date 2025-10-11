{
    inputs = {
        # Get nixpkgs-unstable (see https://status.nixos.org)
        nixpkgs-unstable.url = "github:nixos/nixpkgs/nixos-unstable";

        # Flake-Utils
        flake-utils.url = "github:numtide/flake-utils";

        # Rust Overlay
        rust-overlay = {
            url = "github:oxalica/rust-overlay";
            inputs = { nixpkgs.follows = "nixpkgs-unstable"; };
        };
    };

    outputs = {
        self, flake-utils, nixpkgs-unstable, rust-overlay,
    }:
        flake-utils.lib.eachDefaultSystem (system:
            let
                # Define pkgs as nixpkgs with some additional overlays
                pkgs = import nixpkgs-unstable {
                    inherit system;

                    # Define overlays
                    overlays = [ (import rust-overlay) ];
                };

                # Load Rust toolchain from file
                rustStableToolchain = pkgs.pkgsBuildHost.rust-bin
                    .fromRustupToolchainFile ./rust-toolchain.toml;


                ################################################################
                # Build dependencies
                nativeBuildInputs = with pkgs; [
                    # Rust Toolchain
                    rustStableToolchain

                    # Additional "cargo" commands
                    # cargo-expand       # macro expansion/inspection
                    # cargo-audit        # dependency vulnerability check
                    # cargo-udeps        # dependency inspection
                    # cargo-machete      # -- " --
                    # cargo-depgraph     # -- " --
                    # cargo-deny         # -- " --
                    # cargo-insta        # end-to-end testing/snapshot management

                    # Terminal Code Checker
                    bacon

                    # Linker "mold" (alternative to the default 'cc')
                    # mold

                    # To find system libraries to link against (e.g. libpqxx)
                    pkg-config

                    # Pre-Commit (Framework for managing pre-commit hooks)
                    # pre-commit

                    # Just (make alternative)
                    # just

                    # Nix
                    nil
                ];

                ################################################################
                # Runtime dependencies
                buildInputs = with pkgs; [ openssl ];

                ################################################################
            in
                with pkgs;
            rec {
                devShells.default = mkShell {
                    inherit nativeBuildInputs buildInputs;

                    # shellHook = ''
                    #     pre-commit install
                    # '';

                    # Environment Variables
                    # RUST_BACKTRACE = "full";
                    LD_LIBRARY_PATH = "${lib.makeLibraryPath buildInputs}";
                };
            }
        );
}
