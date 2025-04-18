#!/usr/bin/env bash

set -e

./down.sh

mvn clean package -DskipTests

docker compose build

./startInfra.sh

