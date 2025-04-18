#!/usr/bin/env bash


./startInfra.sh;

docker compose down exchange-1;
docker compose up -d exchange-1; 
sleep 12; 
docker compose logs exchange-1;

docker compose down trader-r;
docker compose up -d trader-r; 
sleep 3 
docker compose logs trader-r
date
sleep 100
date

docker stats --no-stream

date

docker logs trader-r | grep --color=always '11=1|\|11=2399999\|11=2400000\|11=2600000'


date
