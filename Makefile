.PHONY: test
test:
	(cd databuild && python -m pytest test)


.PHONY: lint
lint:
	ruff check --show-fixes databuild

.PHONY: black
black:
	black databuild


.PHONY: format
format:
	ruff check --fix --show-fixes databuild
