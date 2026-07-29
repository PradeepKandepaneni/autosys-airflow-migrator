.PHONY: install test lint migrate report up down clean

install:      ## install engine + dev tools
	pip install -e ".[dev]"

test:         ## run unit tests
	pytest -q

lint:         ## ruff (engine + Airflow 3 upgrade rules)
	ruff check .

migrate:      ## THE one-click: JIL -> Airflow DAGs
	jil2dag migrate --input examples/jil --output out/dags
	@echo "DAGs written to out/dags/"

report:       ## fidelity report only (exit 1 on warnings)
	jil2dag report --input examples/jil

up:           ## local Airflow 3.2 + Datadog agent
	cd docker && docker compose up -d

down:
	cd docker && docker compose down -v

clean:
	rm -rf out .pytest_cache **/__pycache__
