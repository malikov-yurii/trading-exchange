#!/bin/sh

java  \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED  \
  --add-opens=java.base/java.util=ALL-UNNAMED  \
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED  \
  --add-opens=java.base/java.util.zip=ALL-UNNAMED  \
  -Djava.net.preferIPv4Stack=true  \
  -cp /root/jar/app.jar trading.exchange.ExchangeApplication