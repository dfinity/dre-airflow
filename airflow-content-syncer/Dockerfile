FROM fedora:37

RUN dnf install -y bash ca-certificates git rsync

COPY content-syncer /usr/local/bin/content-syncer

RUN mkdir -p /opt/airflow && touch /opt/airflow/.gitkeep

ENTRYPOINT [ "/usr/local/bin/content-syncer" ]
