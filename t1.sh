#!/usr/bin/env bash


./startInfra.sh;

docker compose down exchange-1;
docker compose up -d exchange-1; 
sleep 12; 
docker compose logs exchange-1;

docker compose down trader-1;
docker compose up -d trader-1;
sleep 3 
docker compose logs trader-1
date
sleep 100
date

docker stats --no-stream

date

docker logs trader-1 | grep --color=always '11=1|\|11=2399999\|11=2400000\|11=2600000'


date
