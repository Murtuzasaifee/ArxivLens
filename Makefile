ifneq (,$(wildcard .env))
    include .env
    export
endif

ORCHESTRATOR ?= airflow

.PHONY: help start stop restart status logs health setup format lint test test-cov clean

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# Service management
start: ## Start all services
	docker compose --profile $(ORCHESTRATOR) up --build -d

stop: ## Stop all services
	docker compose --profile airflow --profile prefect down

restart: ## Restart all services
	docker compose --profile $(ORCHESTRATOR) restart

status: ## Show service status
	docker compose ps

logs: ## Show service logs
	docker compose --profile $(ORCHESTRATOR) logs -f

# Health checks
health: ## Check all services health
	@echo "Checking service health..."
	@curl -s http://localhost:8000/health | jq . || echo "API not responding"
	@curl -s http://localhost:9200/_cluster/health | jq . || echo "OpenSearch not responding"
	@if [ "$(ORCHESTRATOR)" = "airflow" ]; then \
		curl -s http://localhost:8080/api/v2/monitor/health || echo "Airflow not responding"; \
	elif [ "$(ORCHESTRATOR)" = "prefect" ]; then \
		curl -s http://localhost:4200/api/health | jq . || echo "Prefect not responding"; \
	fi
	@curl -s http://localhost:11434/api/version | jq . || echo "Ollama not responding"

# Development
setup: ## Install Python dependencies
	uv sync

format: ## Format code
	uv run ruff format

lint: ## Lint and type check
	uv run ruff check --fix
	uv run mypy src/

test: ## Run tests
	uv run pytest

test-cov: ## Run tests with coverage
	uv run pytest --cov=src --cov-report=html

# Cleanup
clean: ## Clean up everything
	docker compose --profile airflow --profile prefect down -v
	docker system prune -f