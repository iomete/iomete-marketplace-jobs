#!/usr/bin/env bash
#
# Builds dependencies.zip for IOMETE Spark-job submission.
#
# IOMETE jobs are packaged as a dependencies.zip bundle submitted alongside
# main.py (no container). This script pins the locked dependency set, installs
# it into a clean build/ directory, and zips it up reproducibly.
#
# Usage:  ./build.sh
# Output: dependencies.zip  (submit this alongside main.py and config.yaml)

set -euo pipefail

cd "$(dirname "$0")"

BUILD_DIR="build"
ZIP_FILE="dependencies.zip"

echo "==> Cleaning previous build artifacts"
rm -rf "$BUILD_DIR" "$ZIP_FILE"
mkdir -p "$BUILD_DIR"

if ! poetry export --help >/dev/null 2>&1; then
    echo "ERROR: 'poetry export' is unavailable. Install the export plugin first:" >&2
    echo "         poetry self add poetry-plugin-export" >&2
    exit 1
fi

echo "==> Exporting locked requirements from poetry.lock"
poetry export -f requirements.txt --without-hashes --output "$BUILD_DIR/requirements.txt"

echo "==> Installing dependencies into $BUILD_DIR"
pip install -r "$BUILD_DIR/requirements.txt" -t "$BUILD_DIR"

echo "==> Creating $ZIP_FILE"
rm -f "$BUILD_DIR/requirements.txt"
(cd "$BUILD_DIR" && zip -q -r "../$ZIP_FILE" .)

echo "==> Done: $ZIP_FILE ($(du -h "$ZIP_FILE" | cut -f1))"
echo "    Submit it alongside main.py and config.yaml as the IOMETE job package."
