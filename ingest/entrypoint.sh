#!/bin/sh
# Railway mounts persistent volumes owned by root. The app runs as the non-root
# "appuser", so fix ownership of the data dir at startup (volumes don't exist at
# build time, so this can't be done in the Dockerfile), then drop privileges via
# gosu. gosu exec's, so the Python process replaces this shell and receives
# SIGTERM directly for clean shutdown.
set -e

DATA_DIR="${SIMPLEX_DATA_DIR:-/data}"
mkdir -p "$DATA_DIR"
chown -R appuser:appuser "$DATA_DIR" 2>/dev/null || true

exec gosu appuser "$@"
