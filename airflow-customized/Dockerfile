FROM apache/airflow:2.7.0-python3.10

# We used to have support for CockroachDB in production.
# No more.
# RUN pip3 install --user sqlalchemy_cockroachdb

# Install support for the Loki log read provider
# as it exists in its github repository.
USER root

RUN \
    apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/log/*

USER airflow

RUN pip3 install --user \
    git+https://github.com/snjypl/airflow-provider-grafana-loki@943e0c8795c4accd09fd38ea371877bc3ec6e424
