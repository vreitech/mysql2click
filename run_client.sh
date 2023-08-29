#!/bin/bash

podman run -it --rm --network host docker.io/clickhouse/clickhouse-client -m
