##########################################
# Makefile to Launchpad GDPR Obfuscator Service - (Linux | GitBash | GitHub Actions)
##########################################

PROJECT_NAME = Obfuscation-Service
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PYTHON = python
PIP = pip
ACTIVATE_ENV := source venv/bin/activate
PYTHON_INTERPRETER = python3

# Helper to run commands inside venv
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

# create python interpreter enviorment.
create-environment:
	@echo ">>> Creating python virtual enviorment for: $(PROJECT_NAME)..."
	@echo ">>> Checking python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up virtualenvironment..."
	( \
		$(PIP) install -q virtualenv virtualenvwrapper; \
		virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)
	@echo ">>> Upgrading pip inside venv..."
	$(call execute_in_env, $(PIP) install --upgrade pip)
	@echo ">>> Virtual environment created successfully!"

# Install all requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r requirements.txt)

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
unit-test:
	$(call execute_in_env, PYTHONPATH=$(PYTHONPATH) $(PYTHON_INTERPRETER) -m pytest tests -vv -s --color=yes)
	@echo ">>> Unit tests completed successfully!"

# Vulnerability check
audit:
	$(call execute_in_env, pip-audit)
	@echo ">>> Vulnerability audit completed successfully!"

# Run coverage check and create a coverage.txt file
check-coverage-txt:
	$(call execute_in_env, PYTHONPATH=$(PYTHONPATH) $(PYTHON_INTERPRETER) -m coverage run -m pytest tests)
	$(call execute_in_env, $(PYTHON_INTERPRETER) -m coverage report -m > coverage.txt)
	@rm -f .coverage
	@echo "Coverage report as coverage.txt created, found in root!"

# Run all tests in one
run-checks: unit-test run-black security-test audit lint
	@echo ">> All checks passed successfully! Obfuscation-Service is PEP8 compliant! <<"