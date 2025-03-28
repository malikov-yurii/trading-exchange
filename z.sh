#!/usr/bin/env bash

docker compose up -d zookeeper
docker run -it --network host bitnami/zookeeper /bin/bash
# Then inside that bash shell:
zkCli.sh -server 127.0.0.1:2181