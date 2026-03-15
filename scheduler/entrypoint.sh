#!/bin/bash
set -e

# Install agent dependencies from mounted volume
for req in /app/agents/*/requirements.txt; do
    [ -f "$req" ] && pip install -q --no-cache-dir -r "$req"
done

# Execute the CMD passed to the container
exec "$@"
