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


FROM apache/airflow:3.0.0

USER airflow

COPY --from=builder /tmp/RADPS/airflow_workflow/dags/* /opt/airflow/dags/
