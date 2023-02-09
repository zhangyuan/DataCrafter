.PHONY: test
test:
	(cd databuild && python -m pytest test)


.PHONY: lint
lint:
	pylint databuild

.PHONY: format
format:
	black databuild
