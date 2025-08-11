ARG PYTHON_VERSION=3.13.5

FROM rust:1.86.0 AS build
ARG PYTHON_VERSION
WORKDIR /app
ENV HOME=/root
ENV PYENV_ROOT=$HOME/.pyenv
ENV PATH=$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH
RUN apt-get update && apt-get install -y protobuf-compiler
RUN curl -fsSL https://pyenv.run | bash
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv global ${PYTHON_VERSION}
RUN pip install maturin==1.8.3
# RUN pip install patchelf==0.17.2.2 # maturin complains about this missing
COPY . .
RUN cd python && maturin build --release

FROM python:${PYTHON_VERSION}-slim
ARG PYTHON_VERSION
WORKDIR /var/lib/raphtory
COPY --from=build /target/wheels/*.whl ./
# ENV VIRTUAL_ENV=/opt/venv
# ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# COPY docker/server.py /home/raphtory_server/server.py

# ENTRYPOINT ["raphtory", "server"]
