FROM openjdk:11-jre-slim

COPY target/trading-participant-0.1.0.jar /app/trading-participant.jar

ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "/app/trading-participant.jar"]