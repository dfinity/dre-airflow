VENV_DIR = venv
VENV_BINDIR = $(VENV_DIR)/bin
.PHONY = test ruff mypy pytest

$(VENV_BINDIR):
	bin/airflow setup
	touch $(VENV_BINDIR)

$(VENV_DIR)/lib/*/site-packages/mock: $(VENV_BINDIR)
	$(VENV_BINDIR)/pip3 install mock
	touch $(VENV_DIR)/lib/*/site-packages/mock

$(VENV_BINDIR)/pytest: $(VENV_BINDIR)
	$(VENV_BINDIR)/pip3 install pytest
	touch $(VENV_BINDIR)/pytest

$(VENV_BINDIR)/mypy: $(VENV_BINDIR)
	$(VENV_BINDIR)/pip3 install mypy types-PyYAML types-requests
	touch $(VENV_BINDIR)/mypy

$(VENV_BINDIR)/ruff: $(VENV_BINDIR)
	$(VENV_BINDIR)/pip3 install ruff
	touch $(VENV_BINDIR)/ruff

mypy: $(VENV_BINDIR)/mypy
	PYTHONPATH=$(PWD)/plugins:$(PWD)/shared MYPY_PATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BINDIR)/mypy --config=mypy.ini

ruff: $(VENV_BINDIR)/ruff
	PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BINDIR)/ruff check shared plugins dags

tests/airflow.db: $(VENV_BINDIR)
	AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__UNIT_TEST_MODE=True AIRFLOW_HOME=$(PWD)/tests PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BINDIR)/airflow db migrate

pytest: tests/airflow.db $(VENV_BINDIR)/pytest $(VENV_DIR)/lib/*/site-packages/mock
	AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__UNIT_TEST_MODE=True AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES_REGEXP="(airflow|dfinity)[.].*" AIRFLOW_HOME=$(PWD)/tests PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BINDIR)/pytest -vv tests

test: ruff mypy pytest
