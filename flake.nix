{
  description = "An example project using flutter";

  inputs.nixpkgs = {
    url = "github:NixOS/nixpkgs";
  };
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.flake-compat = {
    url = "github:edolstra/flake-compat";
    flake = false;
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
          config.android_sdk.accept_license = true;
        };
        pythonEnv = pkgs.python313.withPackages (ps: [
          ps.pip
          ps.virtualenv
        ]);
      in {
        devShells.default = pkgs.mkShell.override {stdenv = pkgs.llvmPackages_21.stdenv;} {
          buildInputs = with pkgs; [
            code-cursor-fhs
            pythonEnv
            antlr
            cmake
            zlib
            zlib.dev
            gdb
          ];

          nativeBuildInputs = [
            pkgs.clang-tools
          ];

          NIX_LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
            pkgs.stdenv.cc.cc
            pkgs.zlib
            pkgs.zlib.dev
          ];

          NIX_LD = pkgs.lib.fileContents "${pkgs.stdenv.cc}/nix-support/dynamic-linker";
          shellHook = ''
    export VENV_DIR="$PWD/.venv"
    if [ ! -d "$VENV_DIR" ]; then
      ${pythonEnv}/bin/python -m venv $VENV_DIR
      source $VENV_DIR/bin/activate
      pip install pip setuptools wheel
    else
      source $VENV_DIR/bin/activate
    fi

    export LD_LIBRARY_PATH=$NIX_LD_LIBRARY_PATH
    export PYTHONPATH="${pythonEnv}/lib/python3.13/site-packages:$PYTHONPATH"

    if [ -f requirements.txt ]; then
      pip install -r requirements.txt
    fi

    python --version
  '';
        };
      });
}
