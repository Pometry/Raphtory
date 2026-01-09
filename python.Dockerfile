ARG RUST_VERSION=1.86.0
ARG PYTHON_VERSION=3.14.2
ARG DEBIAN_VERSION=trixie

FROM rust:${RUST_VERSION} AS build
ARG PYTHON_VERSION
WORKDIR /app
ENV HOME=/root
ENV PYENV_ROOT=$HOME/.pyenv
ENV PATH=$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH
RUN apt-get update && apt-get install -y protobuf-compiler
RUN curl -fsSL https://pyenv.run | bash
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv global ${PYTHON_VERSION}
RUN pip install maturin==1.8.3 patchelf==0.17.2.2
COPY . .
RUN cd python && maturin build --release

FROM python:${PYTHON_VERSION}-slim-${DEBIAN_VERSION}
ARG PYTHON_VERSION
WORKDIR /var/lib/raphtory
COPY --from=build /app/target/wheels/*.whl /
RUN pip install  --no-cache-dir /*.whl && rm /*.whl
ENTRYPOINT ["raphtory", "server"]
