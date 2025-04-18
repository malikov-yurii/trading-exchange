ARG REPO_NAME=docker.io/
ARG IMAGE_NAME=azul/zulu-openjdk-debian
ARG IMAGE_TAG=21
FROM ${REPO_NAME}${IMAGE_NAME}:${IMAGE_TAG}

SHELL [ "/bin/bash", "-o", "pipefail", "-c" ]

COPY --chmod=755 bin /root/bin

RUN /root/bin/setup-docker.sh

WORKDIR /root/jar/

COPY src/main/resources/logback*.xml /root/jar

COPY --chmod=755 target/trading.jar /root/jar/app.jar

ENTRYPOINT ["/root/bin/entrypoint.sh"]
