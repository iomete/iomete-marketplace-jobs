#!/usr/bin/env bash
# Locks pushed image tags against overwrite and deletion, and verifies the lock stuck.
# For the manual make release targets. CI uses the shared step in iomete/.github.
#
# The registry can rewrite tag metadata for a few seconds after a push, which silently
# drops a lock applied in that window, so settle first and read the attribute back
# instead of trusting the update call's own output.
#
# usage: scripts/lock-acr-tag.sh <registry-host>/<repository>:<tag> [more...]
set -euo pipefail

SETTLE_SECONDS=${LOCK_SETTLE_SECONDS:-30}
ATTEMPTS=${LOCK_ATTEMPTS:-5}

echo "Settling ${SETTLE_SECONDS}s before locking"
sleep "$SETTLE_SECONDS"

for reference in "$@"; do
  acr=${reference%%/*}
  acr=${acr%%.azurecr.io}
  image=${reference#*/}
  locked=no
  for i in $(seq 1 "$ATTEMPTS"); do
    az acr repository update --name "$acr" --image "$image" \
      --write-enabled false --delete-enabled false -o none
    sleep 10
    write=$(az acr repository show --name "$acr" --image "$image" \
      --query changeableAttributes.writeEnabled -o tsv)
    delete=$(az acr repository show --name "$acr" --image "$image" \
      --query changeableAttributes.deleteEnabled -o tsv)
    if [ "$write" = "false" ] && [ "$delete" = "false" ]; then
      echo "$image locked (writeEnabled=$write, deleteEnabled=$delete) on attempt $i"
      locked=yes
      break
    fi
    echo "$image not locked on attempt $i (writeEnabled=$write, deleteEnabled=$delete), retrying"
    sleep 10
  done
  if [ "$locked" != "yes" ]; then
    echo "lock did not stick on $image after $ATTEMPTS attempts" >&2
    exit 1
  fi
done
