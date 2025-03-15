#!/usr/bin/env bash

# Build the trading exchange image
docker build -t trading-exchange:0.1.0 -f Dockerfile.exchange .

# Build the trading participant image
docker build -t trading-participant:0.1.0 -f Dockerfile.participant .

