#!/usr/bin/env bash


./startInfra.sh;

docker compose up -d exchange-1;

sleep 2;

docker compose up -d exchange-2;

sleep 6;

docker compose logs exchange-1 exchange-2;

docker compose up -d trader-r;

sleep 7;
clear;

docker compose logs -f exchange-1 exchange-2 trader-r;
