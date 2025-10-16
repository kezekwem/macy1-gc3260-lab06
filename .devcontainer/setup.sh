#!/usr/bin/env bash
set -euo pipefail

cd "${WORKSPACE_FOLDER:-$PWD}"

if [ ! -d ".venv" ]; then
  python -m venv .venv
fi

source .venv/bin/activate
if ! pip install --upgrade pip; then
  echo "[setup] Warning: could not upgrade pip (continuing with bundled version)." >&2
fi
if ! pip install --no-cache-dir -r requirements.txt; then
  echo "[setup] Warning: dependency installation failed. Run 'source .venv/bin/activate && pip install -r requirements.txt' once network access is available." >&2
fi
