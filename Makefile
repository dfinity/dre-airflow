VENV_DIR = venv
VENV_BINDIR = $(VENV_DIR)/bin
.PHONY = tests

$(VENV_BINDIR):
	bin/airflow setup
	touch $(VENV_BINDIR)

$(VENV_BINDIR)/pytest: $(VENV_BINDIR)
	$(VENV_BINDIR)/pip3 install pytest
	touch $(VENV_BINDIR)/pytest

test: $(VENV_BINDIR)/pytest
	PYTHONPATH=$(PWD) $(VENV_BINDIR)/pytest tests
