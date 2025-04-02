FROM eclipse-temurin:11-jre

WORKDIR /aeron

VOLUME ["/aeron/vol"]

COPY *.jar .

ENTRYPOINT ["java", "-Daeron.dir=/aeron/vol/driver", "-Daeron.delete.dir.on.start=true", "-cp", "aeron-all-1.42.1.jar", "io.aeron.driver.MediaDriver"]