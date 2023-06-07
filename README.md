# Airflow setup used by the DRE team

This repository contains code and instructions for working with the
Distributed Reliability Team Airflow instance.


## Deploying an Airflow instance to a VM

The following shellcode deploys Airflow (at a specific version) in a Fedora 38 VM,
using the minimalistic SQLite database.

```
cd /opt/airflow || { mkdir -p /opt/airflow && cd /opt/airflow ; }
dnf install -y python3-pip
test -f bin/pip3 || python3 -m venv .
bin/pip3 install "apache-airflow[celery]==2.6.1" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.7.txt"
export PATH=$PWD/bin:$PATH
export AIRFLOW_HOME="$PWD"/var
```

Updated instructions are available [here](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi).


## Local development environment

TBD.
