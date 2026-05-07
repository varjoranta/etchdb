.PHONY: help install test test-unit test-integration db-up db-down lint typecheck fix build deploy clean
.DEFAULT_GOAL := help

help:
	@echo "etchdb development targets"
	@echo ""
	@echo "  make install           install package + dev deps in a uv venv"
	@echo "  make test              run the full test suite (postgres tests skip if DB is down)"
	@echo "  make test-unit         run only the dialect-neutral unit tests"
	@echo "  make test-integration  bring up postgres, then run integration tests"
	@echo "  make db-up             start the postgres container on localhost:5532"
	@echo "  make db-down           stop and remove the postgres container + volumes"
	@echo "  make lint              check lint + formatting"
	@echo "  make typecheck         run static type checking (ty)"
	@echo "  make fix               auto-format + auto-fix lint"
	@echo "  make build             build sdist + wheel into dist/"
	@echo "  make deploy            build then upload to PyPI (needs UV_PUBLISH_TOKEN)"
	@echo "  make clean             remove build artifacts"

install:
	uv sync --extra dev --extra all

test:
	uv run pytest

test-unit:
	uv run pytest tests/unit -v

test-integration: db-up
	uv run pytest tests/integration -v

db-up:
	docker compose up -d --wait postgres
	@echo "postgres ready on localhost:5532"

db-down:
	docker compose down -v

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run ty check src

fix:
	uv run ruff format .
	uv run ruff check --fix .

build: clean
	uv build

deploy: build
	uv publish

clean:
	rm -rf dist/ build/ src/*.egg-info
