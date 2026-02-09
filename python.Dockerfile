ARG RUST_VERSION=1.86.0
ARG BASE_PYTHON_IMAGE_TAG

FROM rust:${RUST_VERSION} AS build
ARG BASE_PYTHON_IMAGE_TAG
WORKDIR /app
ENV HOME=/root
ENV PYENV_ROOT=$HOME/.pyenv
ENV PATH=$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH
RUN apt-get update && apt-get install -y protobuf-compiler
RUN curl -fsSL https://pyenv.run | bash
RUN PYTHON_VERSION=$(echo ${BASE_PYTHON_IMAGE_TAG} | cut -d'-' -f1) && \
    pyenv install ${PYTHON_VERSION} && \
    pyenv global ${PYTHON_VERSION}
RUN pip install maturin==1.8.3 patchelf==0.17.2.2
COPY . .
RUN cd python && maturin build --release

FROM python:${BASE_PYTHON_IMAGE_TAG}
ARG PYTHON_VERSION
WORKDIR /var/lib/raphtory
COPY --from=build /app/target/wheels/*.whl /
RUN pip install  --no-cache-dir /*.whl && rm /*.whl
VOLUME [ "/var/lib/raphtory" ]

ENTRYPOINT ["raphtory", "server"]
