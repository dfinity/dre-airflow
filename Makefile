VENV_DIR = venv
VENV_BIN_DIR = $(VENV_DIR)/bin
AIRFLOW_SETUP = $(VENV_BIN_DIR)/.airflow-2.11.0
AIRFLOW_CFG = airflow/airflow.cfg
AIRFLOW_TESTS_HOME = $(PWD)/tests
AIRFLOW_TESTS_CFG = $(AIRFLOW_TESTS_HOME)/airflow.cfg
AIRFLOW_TESTS_DB = $(AIRFLOW_TESTS_HOME)/airflow.db

.PHONY = test ruff mypy pytest

$(AIRFLOW_CFG):
	bin/airflow setup

$(AIRFLOW_SETUP): $(AIRFLOW_CFG)
	touch $(AIRFLOW_SETUP)

$(VENV_DIR)/lib/*/site-packages/mock: $(AIRFLOW_SETUP)
	$(VENV_BIN_DIR)/pip3 install mock
	touch $(VENV_DIR)/lib/*/site-packages/mock

$(VENV_BIN_DIR)/pytest: $(AIRFLOW_SETUP)
	$(VENV_BIN_DIR)/pip3 install pytest pytest-mock
	touch $(VENV_BIN_DIR)/pytest

$(VENV_BIN_DIR)/mypy: $(AIRFLOW_SETUP)
	$(VENV_BIN_DIR)/pip3 install mypy types-PyYAML types-requests types-mock
	touch $(VENV_BIN_DIR)/mypy

$(VENV_BIN_DIR)/ruff: $(AIRFLOW_SETUP)
	$(VENV_BIN_DIR)/pip3 install ruff
	touch $(VENV_BIN_DIR)/ruff

mypy: $(VENV_BIN_DIR)/mypy
	PYTHONPATH=$(PWD)/plugins:$(PWD)/shared MYPY_PATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BIN_DIR)/mypy --config=mypy.ini

ruff: $(VENV_BIN_DIR)/ruff
	PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BIN_DIR)/ruff check shared plugins dags

$(AIRFLOW_TESTS_CFG): $(AIRFLOW_SETUP)
	mkdir -p $(AIRFLOW_TESTS_HOME)
	cp -f $(AIRFLOW_CFG) $(AIRFLOW_TESTS_CFG)

tests/airflow.db: $(AIRFLOW_SETUP) $(AIRFLOW_TESTS_CFG)
	AIRFLOW__CORE__EXECUTOR=SequentialExecutor  AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__UNIT_TEST_MODE=True AIRFLOW_HOME=$(AIRFLOW_TESTS_HOME) PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BIN_DIR)/airflow db migrate

pytest: tests/airflow.db $(VENV_BIN_DIR)/pytest $(VENV_DIR)/lib/*/site-packages/mock
	AIRFLOW__CORE__EXECUTOR=SequentialExecutor AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__UNIT_TEST_MODE=True AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES_REGEXP="(airflow|dfinity)[.].*" AIRFLOW_HOME=$(PWD)/tests PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BIN_DIR)/pytest -vv tests

test: ruff mypy pytest
