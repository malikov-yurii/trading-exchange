#!/usr/bin/env bash

set -e

mvn clean package -DskipTests

#docker build --platform=linux/amd64 -t trading-exchange:0.1.0 -f exchange.Dockerfile .
docker build -t trading-exchange:0.1.0 -f exchange.Dockerfile .

#docker build --platform=linux/amd64 -t trading-participant:0.1.0 -f participant.Dockerfile .
docker build -t trading-participant:0.1.0 -f participant.Dockerfile .

