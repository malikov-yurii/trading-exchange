FROM openjdk:11-jre-slim

COPY --chmod=755 target/trading.jar /app/trading.jar

ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "/app/trading.jar"]