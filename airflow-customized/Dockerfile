FROM apache/airflow:2.9.1-python3.12

LABEL org.opencontainers.image.source="https://github.com/dfinity/dre-airflow"

# We need Git.
USER root

RUN \
    apt-get update && \
    apt-get install -y git && \
    apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/log/*

USER airflow

# Disabled.
# Install support for the Loki log read provider
# as it exists in its github repository.
# RUN pip3 install --user \
#     git+https://github.com/snjypl/airflow-provider-grafana-loki@943e0c8795c4accd09fd38ea371877bc3ec6e424
