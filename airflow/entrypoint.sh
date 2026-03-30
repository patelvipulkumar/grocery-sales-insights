#!/bin/bash
set -euo pipefail

# If GOOGLE_APPLICATION_CREDENTIALS_JSON is provided, materialize it at runtime.
if [ -n "${GOOGLE_APPLICATION_CREDENTIALS_JSON:-}" ]; then
    printf '%s' "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > /tmp/gcp-key.json
    chmod 600 /tmp/gcp-key.json
    export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-key.json
fi

# Preserve Apache Airflow base-image startup behavior (dumb-init + /entrypoint).
exec /usr/bin/dumb-init -- /entrypoint "$@"
