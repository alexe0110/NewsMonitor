up:
	docker compose up -d

down:
	docker compose down

pretty:
	uv run --active ruff format .
	uv run --active ruff check --fix --show-fixes .

lint:
	uv run --active ruff check .

plint: pretty lint
