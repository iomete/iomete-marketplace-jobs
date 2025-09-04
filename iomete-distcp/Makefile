# Makefile for iomete-distcp

.PHONY: help install lint format type-check test clean all

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	poetry install

lint: ## Run all linters
	poetry run flake8 iomete_distcp tests
	poetry run mypy iomete_distcp

format: ## Format code with black and isort
	poetry run black iomete_distcp tests
	poetry run isort iomete_distcp tests

format-check: ## Check if code formatting is correct
	poetry run black --check iomete_distcp tests
	poetry run isort --check-only iomete_distcp tests

type-check: ## Run type checking with mypy
	poetry run mypy iomete_distcp

test: ## Run tests
	poetry run pytest tests/ -v

test-cov: ## Run tests with coverage
	poetry run pytest tests/ -v --cov=iomete_distcp --cov-report=html --cov-report=term

clean: ## Clean up build artifacts and cache
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete

all: format lint type-check test ## Run all checks and tests

ci: format-check lint type-check test ## Run CI pipeline checks
