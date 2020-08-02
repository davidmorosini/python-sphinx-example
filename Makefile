default: clean prepare

prepare:
	cd docs/ && sphinx-quickstart

clean:
	rm -rf docs/* && mkdir -p docs

build:
	make html
