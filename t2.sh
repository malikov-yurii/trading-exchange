#!/usr/bin/env bash


./startInfra.sh;

docker compose down exchange-1 trader-1;

docker compose up exchange-1 trader-1;
