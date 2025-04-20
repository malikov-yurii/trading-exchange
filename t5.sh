#!/usr/bin/env bash

export TEST_ID=5

export EXCHANGE_JAVA_TOOL_OPTIONS="\
-Dlogback.configurationFile=/root/jar/logback.xml \
-XX:+TieredCompilation -XX:TieredStopAtLevel=4 -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:+UseStringDeduplication \
-Xms2G -Xmx4G"

export TRADER_JAVA_TOOL_OPTIONS="\
-Dlogback.configurationFile=/root/jar/logback.xml \
-XX:+TieredCompilation -XX:TieredStopAtLevel=4 -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:+UseStringDeduplication \
-Xms2G -Xmx4G"

docker compose up -d trader-r

./startInfra.sh;

date
docker compose up -d exchange-1;
date
sleep 2;
docker compose up -d exchange-2 exchange-3;
date
sleep 9;
docker compose up -d trader-r;
date
sleep 15
date
docker compose pause exchange-1
date
sleep 15
date
echo

#docker compose logs -f

docker compose logs | grep Resend;
ORDER_ID=$(docker compose logs | grep 'Resend' | grep -oE '\|11=[0-9]+\|' | head -1 | cut -d'=' -f2 | tr -d '|');
KEY="|11=$ORDER_ID|\|LEADER\|FOLLOWER";
echo "Extracted ORDER_ID is: [$ORDER_ID]. KEY: [$KEY]";
echo
echo "compose logs trader-r | grep --color=always '$KEY'"
docker compose logs trader-r | grep --color=always "$KEY";
echo
echo "compose logs exchange-1 | grep --color=always '$KEY'"
docker compose logs exchange-1 | grep --color=always "$KEY";
echo
echo "compose logs exchange-2 | grep --color=always '$KEY'"
docker compose logs exchange-2 | grep --color=always "$KEY";
echo
echo "compose logs exchange-3 | grep --color=always '$KEY'"
docker compose logs exchange-3 | grep --color=always "$KEY";
echo
date