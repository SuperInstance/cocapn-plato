.PHONY: test coverage coverage-gaps generate-tests lint fmt security docker-build docker-run install clean

PYTHON := python3
PIP := $(PYTHON) -m pip

install:
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	$(PIP) install litellm>=1.0.0

test:
	$(PYTHON) -m pytest -x -v

coverage:
	$(PYTHON) -m pytest -x -v --cov=cocapn_plato --cov-report=term --cov-report=html --cov-report=xml --cov-report=json
	$(PYTHON) -m coverage report --fail-under=75

coverage-gaps:
	$(PYTHON) -m pytest tests/ --cov=cocapn_plato --cov-report=json -q || true
	$(PYTHON) -m scripts.cover_agent.generate_tests --analyze-only

generate-tests:
	@if [ -z "$(OPENAI_API_KEY)" ] && [ -z "$(LITELLM_API_KEY)" ]; then \
		echo "Error: OPENAI_API_KEY or LITELLM_API_KEY required"; \
		exit 1; \
	fi
	$(PYTHON) -m scripts.cover_agent.generate_tests --max-gaps 10 --model $(or $(LITELLM_MODEL),gpt-4o-mini) --budget 5.0

validate-tests:
	$(PYTHON) -m scripts.cover_agent.validate_tests --all

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
