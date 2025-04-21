#!/usr/bin/env bash

set -e

clear

docker compose stop trader-1


mvn clean package -DskipTests

docker compose build trader-1

docker compose up trader-1

