#!/bin/bash

set -euo pipefail
IFS=$'\t\n'

cd "$(realpath "$(dirname "$0")")"

USE_DOCKER=y
REBUILD=n
CURL_CONNECT_TIMEOUT=5
TARGET_IMAGE_NAME='mysql2click'
TARGET_IMAGE_URI='localhost/'"${TARGET_IMAGE_NAME}"':latest'

HELP_MESSAGE='Command format:
-p  | --use-podman   Use Podman instead of Docker
-r  | --rebuild      Rebuild image if another one with the same name is found in the registry
-h  | --help         Show that help message
'

for opt in "$@"; do
  case ${opt} in
    -p|--use-podman)
      USE_DOCKER=n
      shift
      ;;
    -r|--rebuild)
      REBUILD=y
      shift
      ;;
    -h|--help)
      echo -e "${HELP_MESSAGE}" && exit 0
      shift
      ;;
    *)
      echo -e "Unknown command argument.\n${HELP_MESSAGE}" && exit 1
      ;;
  esac
done

shopt -s nocasematch

if [[ ${USE_DOCKER} != y ]]; then
  echo "... Enabling user's podman socket."
  systemctl --user reload-or-restart podman.socket
  if [[ $? -ne 0 ]]
  then
    >&2 echo "... Status code equal to $? which is different from 0, aborting."
    exit 1
  fi
fi

echo "... Checking existance of image ${TARGET_IMAGE_URI}"
podman_target_images_count=$(curl --connect-timeout ${CURL_CONNECT_TIMEOUT} \
  -sLXGET -H"Accept: application/vnd.oci.image.manifest.v1+json" \
  --unix-socket $([[ ${USE_DOCKER} != y ]] && echo "${XDG_RUNTIME_DIR}/podman/podman.sock" || echo "/run/docker.sock") \
  $([[ ${USE_DOCKER} != y ]] && echo 'http://d/v4.0.0/libpod/images/json' || echo 'http://latest/images/json') \
  | jq -r '[.[] | select(.RepoTags[]? == "localhost/mysql2click:latest" or .RepoTags[]? == "mysql2click:latest") | .Id] | length')
if [[ $? -ne 0 ]]
then
  >&2 echo "... Status code equal to $? which is different from 0, aborting."
  exit 1
fi

if [[ ${podman_target_images_count} -eq 0 ]]
then
  echo "... Image ${TARGET_IMAGE_NAME} not found."
else
  echo "... Image ${TARGET_IMAGE_NAME} is found."
  if [[ ${REBUILD} == "y" ]]
  then
    echo "... '--rebuild' parameter is present."
  else
    echo "... Run '$([[ ${USE_DOCKER} != y ]] && echo podman || echo docker) rmi ${TARGET_IMAGE_NAME}:latest' first if you need to rebuild it, or use '--rebuild' parameter."
  fi
fi

if [[ ${podman_target_images_count} -eq 0 || ${REBUILD} == "y" ]]
  echo "... Building new image."
  bash -c "$([[ ${USE_DOCKER} != y ]] && echo "buildah bud -t ${TARGET_IMAGE_NAME}" || echo "docker build -t ${TARGET_IMAGE_NAME} -f ./Containerfile .")"
  if [[ $? -ne 0 ]]
  then
    >&2 echo "... Status code equal to $? which is different from 0, aborting."
    exit 1
  fi
fi