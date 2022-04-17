install-dev-requirements:
	pip install -r dev-requirements.txt

mypy:
	mypy

black:
	black .

build:
	rm -rf /tmp/cloud_validol
	mkdir /tmp/cloud_validol
	python -m build -o /tmp/cloud_validol/build

upload:
	twine upload /tmp/cloud_validol/build/*.tar.gz
