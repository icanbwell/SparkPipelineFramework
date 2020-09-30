LANG=en_US.utf-8

export LANG

BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
VERSION=$(shell cat VERSION)
VENV_NAME=venv_sparkpipelineframework
GIT_HASH=${CIRCLE_SHA1}
SPARK_VER=3.0.1
HADOOP_VER=3.2

include Makefile.spark
include Makefile.docker

.PHONY:venv
venv:
	python3 -m venv $(VENV_NAME)

.PHONY:devsetup
devsetup:venv
	source $(VENV_NAME)/bin/activate && \
    pip install --upgrade pip && \
    python setup.py install && \
    pip install --upgrade -r requirements.txt && \
    pip install --upgrade -r requirements-test.txt

.PHONY:check
check:venv
	source $(VENV_NAME)/bin/activate && \
    pip install --upgrade -r requirements.txt && \
    mypy spark_pipeline_framework

.PHONY:buildpackage
buildpackage:venv
	source $(VENV_NAME)/bin/activate && \
    pip install --upgrade pip && \
    python setup.py install && \
    pip install --upgrade -r requirements.txt && \
    rm -r dist/ && \
    python3 setup.py sdist bdist_wheel

.PHONY:testpackage
testpackage:venv buildpackage
	python3 -m twine upload -u __token__ --repository testpypi dist/*
# password can be set in TWINE_PASSWORD. https://twine.readthedocs.io/en/latest/

.PHONY:package
package:venv buildpackage
	python3 -m twine upload -u __token__ --repository pypi dist/*
# password can be set in TWINE_PASSWORD. https://twine.readthedocs.io/en/latest/

.PHONY:test
test:
	source $(VENV_NAME)/bin/activate && \
	pytest tests

.PHONY:firsttime
firsttime: installspark docker up devsetup proxies test

.PHONY:proxies
proxies:
	python3 spark_pipeline_framework/proxy_generator/generate_proxies.py
