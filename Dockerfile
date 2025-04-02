FROM openjdk:11-jre-slim

COPY target/trading-0.1.0.jar /app/trading.jar

ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "/app/trading.jar"]