#!/usr/bin/env bash


./startInfra.sh;

docker compose up -d exchange-1;
sleep 2;
docker compose up -d exchange-2 exchange-3;
sleep 10;
docker compose logs exchange-1  exchange-1;
sleep 4;
docker compose up -d trader-r;
sleep 3 
docker compose logs -f
date

#id=333; docker compose logs | grep trader-r |grep --color=always "|11=$id\|Id=$id" | grep '39=\|'