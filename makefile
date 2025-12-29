##########################################
# Makefile to Launchpad GDPR Obfuscator Service 
##########################################

PROJECT_NAME = Obfuscation-Service
REGION = eu-west-2
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash

# Detect Operasion System
ifeq ($(OS),Windows_NT)
    # Windows
	OS := Windows
    ACTIVATE_ENV := source venv/Scripts/activate
    BIN_DIR := Scripts
    PYTHON_INTERPRETER := python
    PIP := python -m pip
else
    # Linux / GitHub Actions
	OS := Linux
    ACTIVATE_ENV := source venv/bin/activate
    BIN_DIR := bin
    PYTHON_INTERPRETER := python3
    PIP := python3 -m pip
endif

# Helper to run commands inside venv
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

# create python interpreter enviorment.
# Built in venv helps to run on either Linuxon or Windowson withouth extra installation
create-environment:
	@echo ">>> Creating python virtual environment for: $(PROJECT_NAME)..."
	@echo ">>> Operating System detected: $(OS)"
	@echo ">>> Checking python version:"
	$(PYTHON_INTERPRETER) --version
	@echo ">>> Setting up virtualenvironment..."
	$(PYTHON_INTERPRETER) -m venv venv
	@echo ">>> Upgrading pip inside venv..."
	$(ACTIVATE_ENV) && python -m pip install --upgrade pip
	@echo ">>> Virtual environment created successfully!"

# Install all requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r requirements.txt)
	@echo ">>> All requirements installed successfully!"

##################################################################
# Quality, Security & Testing

# Run bandit (security scanner) on every python file
security-test:
	$(call execute_in_env, bandit -lll -r */*.py)
	@echo ">>> Security scan completed successfully!"

# Run black (code formatter)
run-black:
	$(call execute_in_env, black ./src ./tests)
	@echo ">>> Code formatted successfully!"

# Run flake8 (code linter)
lint:
	$(call execute_in_env, flake8 --max-line-length=100 src tests)
	@echo ">>> Linting completed successfully!"

# Run tests
# $(call execute_in_env, PYTHONPATH=$(PYTHONPATH) pytest tests --testdox -vvrP)
unit-test:
	$(call execute_in_env, PYTHONPATH=$(PYTHONPATH) python -m pytest tests -vv -s --color=yes)
	@echo ">>> Unit tests completed successfully!"

# Vulnerability check
audit:
	$(call execute_in_env, pip-audit)
	@echo ">>> Vulnerability audit completed successfully!"

# Run coverage check and create a coverage.txt file in the data folder
check-coverage-txt:
	$(call execute_in_env, PYTHONPATH=$(PYTHONPATH) coverage run -m pytest tests)
	$(call execute_in_env, PYTHONPATH=$(PYTHONPATH) coverage report -m > coverage.txt)
	@rm -f .coverage
	@echo "Coverage report as coverage.txt created, found in root!"

# Run all tests in one
run-checks: unit-test run-black security-test audit lint
	@echo ">> All checks passed successfully! Obfuscation-Service is PEP8 compliant! <<"