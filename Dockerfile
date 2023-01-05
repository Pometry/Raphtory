FROM sbtscala/scala-sbt:eclipse-temurin-11.0.17_8_1.8.1_2.13.10 as python-build
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
COPY pyproject.toml pyproject.toml
COPY setup.cfg setup.cfg
COPY MANIFEST.in MANIFEST.in
RUN apt update
RUN apt install -y make wget unzip
RUN apt install -y python3 python3-pip python3-distutils
RUN apt install -y python3-venv
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN make clean python-build



# =============================
FROM eclipse-temurin:11.0.17_8-jre-jammy as raphtory-core
RUN apt update
RUN apt install -y python3 libpython3.10
RUN ln -s /usr/lib/x86_64-linux-gnu/libpython3.10.so.1 /usr/lib/x86_64-linux-gnu/libpython3.so
RUN mkdir -p /raphtory/jars
RUN mkdir -p /raphtory/bin
RUN mkdir -p /tmp
COPY bin/docker/raphtory/bin /raphtory/bin
COPY --from=python-build /opt/venv /opt/venv
COPY it/src/test/resources /raphtory/jars
ENV PATH="/opt/venv/bin:$PATH"
ENV PYEXEC="/opt/venv/bin/python3"

CMD /raphtory/bin/entrypoint.sh

