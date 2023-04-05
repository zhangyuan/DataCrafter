.PHONY: test
test:
	(cd databuild &&  python -m pytest -v tests --cov=.)

.PHONY: coverage
coverage:
	(cd databuild &&  python -m pytest test -v --cov=. > pytest-coverage.txt )


.PHONY: lint
lint:
	ruff check --show-fixes databuild

.PHONY: black
black:
	black databuild


.PHONY: format
format:
	ruff check --fix --show-fixes databuild
