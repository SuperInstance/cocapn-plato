.PHONY: test coverage lint security docker-build docker-run install clean

PYTHON := python3
PIP := $(PYTHON) -m pip

install:
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

test:
	$(PYTHON) -m pytest -x -v

coverage:
	$(PYTHON) -m pytest -x -v --cov=cocapn_plato --cov-report=term --cov-report=html --cov-report=xml
	$(PYTHON) -m coverage report --fail-under=75

lint:
	$(PYTHON) -m ruff check src tests scripts
	$(PYTHON) -m ruff format --check src tests scripts
	$(PYTHON) -m mypy src || true

fmt:
	$(PYTHON) -m ruff format src tests scripts
	$(PYTHON) -m ruff check --fix src tests scripts

security:
	$(PYTHON) -m bandit -r src
	$(PYTHON) -m pip_audit --desc

docker-build:
	docker build -t cocapn-plato:latest .

docker-run:
	docker run --rm -p 8000:8000 cocapn-plato:latest

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete
	find . -type f -name '*.pyo' -delete
	find . -type f -name '*.egg-info' -delete
	rm -rf .pytest_cache htmlcov .coverage coverage.xml dist build
