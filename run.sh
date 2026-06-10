#!/usr/bin/env bash
# Launch the First Faults GUI on localhost:5001 so the redirect URI
# matches what Google Cloud Console has registered.
set -euo pipefail
cd "$(dirname "$0")"
source ./set_env.sh
exec .venv/bin/flask --app presentations.app run --port "${1:-5002}"
