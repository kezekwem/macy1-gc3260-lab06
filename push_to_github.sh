#!/usr/bin/env bash
set -euo pipefail

GH_USER="kezekwem"
REPO_NAME="macy1-gc3260-lab06"
GIT_ROOT="/Users/kenechukwuezekwem/Documents/MACY1_GC_3260_LAB06_V2.0_10152025"

cd "$GIT_ROOT"

git init
git add .
git commit -m "Initial lab import"
git branch -M main

REMOTE_URL="https://github.com/${GH_USER}/${REPO_NAME}.git"

if ! command -v gh >/dev/null 2>&1; then
  echo "[ERROR] GitHub CLI (gh) is not installed. Install it from https://cli.github.com/ and run 'gh auth login' before rerunning." >&2
  exit 1
fi

if ! gh repo view "${GH_USER}/${REPO_NAME}" >/dev/null 2>&1; then
  gh repo create "${GH_USER}/${REPO_NAME}" --public --confirm
fi

git remote remove origin >/dev/null 2>&1 || true
git remote add origin "$REMOTE_URL"
git push -u origin main
