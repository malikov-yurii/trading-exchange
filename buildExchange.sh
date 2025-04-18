#!/usr/bin/env bash

clear 

set -e

docker compose stop exchange-1


mvn clean package -DskipTests

docker compose build exchange-1

docker compose up exchange-1

