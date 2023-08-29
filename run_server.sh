#!/bin/bash

podman run -d --name mysql2click-clickhouse \
  -p 9000:9000 \
  -v mysql2click-clickhouse-volume:/var/lib/clickhouse \
  --shm-size 2g \
  --tz 'Europe/Moscow' \
  docker.io/clickhouse/clickhouse-server:22.8

