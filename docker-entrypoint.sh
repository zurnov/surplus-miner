#!/bin/sh
# Populate known_hosts for MINER_HOSTS to avoid Host key verification failed.
set -e

if [ -n "$MINER_HOSTS" ]; then
  IFS=','; for h in $MINER_HOSTS; do
    h=$(echo "$h" | tr -d ' ')
    if [ -n "$h" ]; then
      # Append host key if not already present
      if ! ssh-keygen -F "$h" >/dev/null 2>&1; then
        ssh-keyscan -H "$h" >> /root/.ssh/known_hosts 2>/dev/null || true
      fi
    fi
  done
fi

# Exec main command
exec "$@"