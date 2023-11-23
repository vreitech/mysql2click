#!/bin/bash

set -euo pipefail
shopt -s nocasematch
IFS=$'\t\n'

cd "$(realpath "$(dirname "$0")")"

USE_DOCKER=y
CURL_CONNECT_TIMEOUT=5
TARGET_IMAGE_NAME='mysql2click'
TARGET_IMAGE_URI='localhost/'"${TARGET_IMAGE_NAME}"':latest'

if [[ ${USE_DOCKER} != y ]]; then
  echo "--- Enabling user's podman socket"
  systemctl --user reload-or-restart podman.socket
  if [[ $? -ne 0 ]]
  then
    >&2 echo "--- Status code equal to $? which is different from 0, aborting"
    exit 1
  fi
fi

echo "--- Checking existance of image ${TARGET_IMAGE_URI}"
podman_target_images_count=$(curl --connect-timeout ${CURL_CONNECT_TIMEOUT} \
  -sLXGET -H"Accept: application/vnd.oci.image.manifest.v1+json" \
  --unix-socket $([[ ${USE_DOCKER} != y ]] && echo "${XDG_RUNTIME_DIR}/podman/podman.sock" || echo "/run/docker.sock") \
  $([[ ${USE_DOCKER} != y ]] && echo 'http://d/v4.0.0/libpod/images/json' || echo 'http://latest/images/json') \
  | jq -r '[.[] | select(.RepoTags[]? == "localhost/mysql2click:latest" or .RepoTags[]? == "mysql2click:latest") | .Id] | length')
if [[ $? -ne 0 ]]
then
  >&2 echo "--- Status code equal to $? which is different from 0, aborting"
  exit 1
fi
if [[ ${podman_target_images_count} -eq 0 ]]
then
  echo "--- Image ${TARGET_IMAGE_NAME} not found, building"
  bash -c "$([[ ${USE_DOCKER} != y ]] && echo "buildah bud -t ${TARGET_IMAGE_NAME}" || echo "docker build -t ${TARGET_IMAGE_NAME} -f ./Containerfile .")"
  if [[ $? -ne 0 ]]
  then
    >&2 echo "--- Status code equal to $? which is different from 0, aborting"
    exit 1
  fi
else
  echo "--- Image ${TARGET_IMAGE_NAME} is found"
fi

echo "--- Starting container"
$([[ ${USE_DOCKER} != y ]] && echo podman || echo docker) run -it --rm \
  --network host \
  $([[ ${USE_DOCKER} != y ]] && echo "--tz 'Europe/Moscow'") \
  --shm-size=1gb \
  -v $(pwd)/config.ini:/usr/src/myapp/config.ini "${TARGET_IMAGE_NAME}" \
  python3 ./mysql2click.py
