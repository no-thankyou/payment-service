.PHONY: install lint fmt typecheck test test-unit test-int check up up-dev down logs e2e

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy src tests scripts

test:
	uv run pytest

test-unit:
	uv run pytest -m "not integration"

test-int:
	uv run pytest -m integration

check: lint typecheck test

up:
	docker compose up -d --build --wait

up-dev:
	docker compose --profile dev up -d --build --wait

down:
	docker compose --profile dev down -v

logs:
	docker compose logs -f api consumer

e2e:
	uv run python scripts/e2e_smoke.py
