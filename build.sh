#!/usr/bin/env bash

set -e

mvn clean package -DskipTests

docker-compose build

