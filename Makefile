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

test: $(VENV_BINDIR)/pytest $(VENV_DIR)/lib/*/site-packages/mock
	PYTHONPATH=$(PWD)/plugins:$(PWD)/shared $(VENV_BINDIR)/pytest -v tests
