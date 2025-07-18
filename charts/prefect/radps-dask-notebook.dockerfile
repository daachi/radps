FROM ubuntu:latest AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    autoconf \
    git \
    wget

WORKDIR /tmp

RUN git clone https://github.com/casangi/RADPS.git


FROM daskdev/dask-notebook:2025.7.0-py3.12

COPY --from=builder /tmp/RADPS/prefect_workflow /home/jovyan/prefect_workflow
