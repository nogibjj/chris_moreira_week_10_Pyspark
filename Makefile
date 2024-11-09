install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:
	black *.py mylib/*.py


lint:
	# pylint --load-plugins=pylint_pyspark --disable=R,C --ignore-patterns=test_.*?py *.py mylib/*.py

	# PYTHONPATH=$(shell pwd) pylint --disable=R,C --ignore-patterns=test_.*?py *.py mylib/*.py

	#disable comment to test speed
	# pylint --disable=R,C --ignore-patterns=test_.*?py *.py mylib/*.py
	#ruff linting is 10-100X faster than pylint
	ruff check *.py mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

deploy:
	# Place deploy statement

refactor: format lint

all: install lint test format 
