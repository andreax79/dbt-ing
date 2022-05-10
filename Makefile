SHELL=/bin/bash -e

help:
	@echo "- make build        Build package"
	@echo "- make clean        Clean"
	@echo "- make tag          Create version tag"
	@echo "- make test         Run tests"

tag:
	@git tag -a "v$$(cat dbting/VERSION)" -m "version v$$(cat dbting/VERSION)"

build: clean
	python3 setup.py bdist_wheel
	python3 setup.py sdist bdist_wheel

clean:
	-rm -rf build dist
	-rm -rf *.egg-info

test:
	python3 setup.py test
