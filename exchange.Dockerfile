FROM openjdk:11-jre-slim

COPY target/trading-exchange-0.1.0.jar /app/trading-exchange.jar

ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "/app/trading-exchange.jar"]