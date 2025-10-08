
SHELL = /bin/bash
.DEFAULT_GOAL = install

VENV = ./.venv/bin/activate
PY = ./.venv/bin/python -m
PY_LOCK = ./.venv-lock/bin/python -m

pkg_src = ./api ./inbound
test_src = ./tests
dbt_src = ./dbt/models ./dbt/macros ./dbt/tests

isort = $(PY) isort $(pkg_src) $(tests_src)
black = $(PY) black $(pkg_src) $(tests_src)
sqlfmt = $(PY) sqlfmt $(dbt_src)

.PHONY: install ## install requirements in virtual env
install:
	rm -rf .venv
	python3.11 -m venv .venv && \
		${PY} pip install --upgrade pip && \
		${PY} pip install -r requirements-lock.txt -r requirements-dev.txt
	rm -rf elementary/.venv
	python3.11 -m venv elementary/.venv && \
		./elementary/.venv/bin/python -m pip install --upgrade pip && \
		./elementary/.venv/bin/python -m pip install -r elementary/requirements.txt

_lock-file:
	python3.11 -m venv .venv-lock && \
		${PY_LOCK} pip install --upgrade pip && \
		${PY_LOCK} pip install -r requirements.txt && \
		${PY_LOCK} pip freeze > requirements-lock.txt
	rm -rf .venv-lock

.PHONY: lock-file ## Create pip-lockfile and install its dependencies
lock-file: _lock-file install

.PHONY: report ## Create elementary report
report:
	. elementary/.venv/bin/activate; edr report --profiles-dir dbt

.PHONY: all  ## Perform the most common development-time rules
all: format tests

.PHONY: format  ## Auto-format the source code (isort, black)
format:
	$(isort)
	$(black)
	$(sqlfmt)

.PHONY: tests  ## Run tests
tests:
	PYTHONPATH=. pytest ./tests/unit

.PHONY: inbound-tests ## Run inbound tests
inbound-tests:
	PYTHONPATH=. pytest ./inbound/tests

start-colima:
	colima start --arch x86_64 --memory 6

stop-colima:
	colima stop

monday_patch: lock-file
