#!/bin/bash

set -euo pipefail
IFS=$'\t\n'

CURL_CONNECT_TIMEOUT=5
TARGET_IMAGE_NAME='mysql2click'
TARGET_IMAGE_URI='localhost/'"${TARGET_IMAGE_NAME}"':latest'

echo "--- Enabling user's podman socket"
systemctl --user reload-or-restart podman.socket
if [[ $? -ne 0 ]]
then
  >&2 echo "--- Status code equal to $? which is different from 0, aborting"
  exit 1
fi

echo "--- Checking existance of image ${TARGET_IMAGE_URI}"
podman_target_images_count=$(curl --connect-timeout ${CURL_CONNECT_TIMEOUT} \
  -sLXGET -H"Accept: application/vnd.oci.image.manifest.v1+json" \
  --unix-socket "${XDG_RUNTIME_DIR}/podman/podman.sock" \
  'http://d/v4.0.0/libpod/images/json' \
  | jq -r '[.[] | select(.RepoTags[]? == "localhost/mysql2click:latest") | .Id] | length')
if [[ $? -ne 0 ]]
then
  >&2 echo "--- Status code equal to $? which is different from 0, aborting"
  exit 1
fi
if [[ ${podman_target_images_count} -eq 0 ]]
then
  echo "--- Image ${TARGET_IMAGE_NAME} not found in podman, building"
  buildah bud -t "${TARGET_IMAGE_NAME}"
  if [[ $? -ne 0 ]]
  then
    >&2 echo "--- Status code equal to $? which is different from 0, aborting"
    exit 1
  fi
else
  echo "--- Image ${TARGET_IMAGE_NAME} is found"
fi

echo "--- Starting container"
podman run -it --rm \
  --network host \
  --tz 'Europe/Moscow' \
  -v $(pwd):/usr/src/myapp "${TARGET_IMAGE_NAME}" \
  python3 ./mysql2click.py
