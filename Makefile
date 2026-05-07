.PHONY: help install test lint fix build deploy clean
.DEFAULT_GOAL := help

help:
	@echo "etchdb development targets"
	@echo ""
	@echo "  make install     install package + dev deps in a uv venv"
	@echo "  make test        run the test suite"
	@echo "  make lint        check lint + formatting"
	@echo "  make typecheck   run static type checking (ty)"
	@echo "  make fix         auto-format + auto-fix lint"
	@echo "  make build       build sdist + wheel into dist/"
	@echo "  make deploy      build then upload to PyPI (needs UV_PUBLISH_TOKEN)"
	@echo "  make clean       remove build artifacts"

install:
	uv sync --extra dev --extra all

test:
	uv run pytest

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
