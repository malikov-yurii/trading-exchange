#!/usr/bin/env bash

set -e

clear

docker compose stop trader-r


mvn clean package -DskipTests

docker compose build trader-r

docker compose up trader-r

