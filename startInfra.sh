#!/usr/bin/env bash

docker compose up -d zookeeper archive-host;

sleep 1;

docker compose logs zookeeper archive-host;

docker ps

