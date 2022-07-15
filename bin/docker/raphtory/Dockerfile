ARG JAVA_DOCKER_IMAGE_NAME=eclipse-temurin
ARG JAVA_DOCKER_IMAGE_TAG=11.0.12_7-jdk
ARG RUN_USER=root
FROM ${JAVA_DOCKER_IMAGE_NAME}:${JAVA_DOCKER_IMAGE_TAG}
RUN mkdir -p /raphtory/
RUN mkdir -p /raphtory/data
RUN mkdir -p /tmp/raphtory/data/
COPY bin/docker/raphtory/bin /raphtory/bin
COPY tmp/builder/raphtory/jars /raphtory/jars
RUN chmod 777 /raphtory/bin/entrypoint.sh
CMD /raphtory/bin/entrypoint.sh
USER ${RUN_USER}
