FROM hseeberger/scala-sbt:eclipse-temurin-11.0.14.1_1.6.2_2.13.8 as sbt-builder
COPY core core/
COPY examples examples/
COPY project project/
COPY deploy deploy/
COPY python python/
COPY arrow-core arrow-core/
COPY arrow-messaging arrow-messaging/
COPY connectors connectors/
COPY build.sbt build.sbt
COPY .scalafmt.conf .scalafmt.conf
COPY Makefile Makefile
COPY version version
RUN apt update
RUN apt install -y make wget unzip
RUN make sbt-build

FROM eclipse-temurin:11.0.15_10-jdk as python-build
RUN apt update
RUN apt install -y python3 python3-pip python3-distutils
RUN apt install -y python3-venv
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY --from=sbt-builder /root/python python
COPY Makefile Makefile
COPY version version
RUN make python-build -W sbt-build

# =============================
FROM eclipse-temurin:11.0.17_8-jre-jammy as raphtory-core
RUN apt update
RUN apt install -y python3 libpython3.10
RUN ln -s /usr/lib/x86_64-linux-gnu/libpython3.10.so.1 /usr/lib/x86_64-linux-gnu/libpython3.so
RUN mkdir -p /raphtory/jars
RUN mkdir -p /raphtory/bin
RUN mkdir -p /raphtory/data
RUN mkdir -p /tmp
COPY bin/docker/raphtory/bin /raphtory/bin
COPY --from=python-build /opt/venv /opt/venv
COPY it/src/test/resources /raphtory/jars
ENV PATH="/opt/venv/bin:$PATH"
ENV PYEXEC="/opt/venv/bin/python3"

CMD /raphtory/bin/entrypoint.sh

