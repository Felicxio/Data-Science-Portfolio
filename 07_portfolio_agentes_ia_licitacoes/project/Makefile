.PHONY: install test lint security seed run

install:
	uv sync                  # instala prod + dev a partir do uv.lock
	uv run pre-commit install

test:
	uv run pytest tests/unit/ -v

test-all:
	uv run pytest --cov=licitacao --cov-fail-under=70

lint:
	uv run ruff check . && uv run ruff format --check .

security:
	uv run bandit -r licitacao/ -ll
	uv run pip-audit

seed:
	uv run python knowledge/seeds/seed_kb.py

run:
	uv run python -m licitacao.graph