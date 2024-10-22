FROM python:3.12.4-slim

# Install packages & python base
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    protobuf-compiler \
    curl \
    gcc \
    g++ \
    make \
    git \
    libssl-dev \
    patchelf && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a virtual environment
RUN python3 -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install Rustup
RUN curl --proto '=https' --tlsv1.3 https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup toolchain install stable
RUN cargo install --locked maturin
WORKDIR /home/raphtory_server

ENTRYPOINT [ "/bin/sh"]