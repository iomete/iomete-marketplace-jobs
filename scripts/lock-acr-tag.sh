#!/usr/bin/env bash
# Locks ACR image tags against overwrite and deletion, and verifies the lock stuck.
#
# ACR can rewrite tag metadata for a few seconds after a push, which silently drops
# a lock applied in that window, so settle first and read the attribute back instead
# of trusting the update call's own output.
#
# usage: scripts/lock-acr-tag.sh <acr-name> <repository> <tag> [tag...]
set -euo pipefail

SETTLE_SECONDS=${LOCK_SETTLE_SECONDS:-30}
ATTEMPTS=${LOCK_ATTEMPTS:-5}

acr=$1
repository=$2
shift 2

echo "Settling ${SETTLE_SECONDS}s before locking"
sleep "$SETTLE_SECONDS"

for tag in "$@"; do
  image="${repository}:${tag}"
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
    echo "::error::lock did not stick on $image after $ATTEMPTS attempts"
    exit 1
  fi
done
