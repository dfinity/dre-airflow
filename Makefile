VENV_DIR = venv
VENV_BINDIR = $(VENV_DIR)/bin
.PHONY = tests

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

test: $(VENV_BINDIR)/pytest $(VENV_BINDIR)/mypy $(VENV_BINDIR)/ruff $(VENV_DIR)/lib/*/site-packages/mock
	PYTHONPATH=$(PWD)/plugins:$(PWD)/shared MYPY_PATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BINDIR)/mypy --config=mypy.ini
	PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BINDIR)/ruff check shared plugins dags
	PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BINDIR)/pytest -v tests
