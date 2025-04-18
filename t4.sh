#!/usr/bin/env bash


./startInfra.sh;

docker compose up -d exchange-1;
sleep 2;
docker compose up -d exchange-2;
sleep 10;
docker compose logs exchange-1  exchange-1;
sleep 4;
docker compose up -d trader-r;
sleep 3 
docker compose logs trader-r
date
sleep 110
date

docker stats --no-stream

date

docker logs trader-r | grep --color=always '11=1|\|11=2399999\|11=2400000\|11=2600000'


date
