LANG=en_US.utf-8

export LANG

BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
VERSION=$(shell cat VERSION)
VENV_NAME=venv_sparkpipelineframework
GIT_HASH=${CIRCLE_SHA1}
SPARK_VER=3.0.1
HADOOP_VER=3.2

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

.PHONY:testpackage
testpackage:venv
	source $(VENV_NAME)/bin/activate && \
    pip install --upgrade pip && \
    python setup.py install && \
    pip install --upgrade -r requirements.txt && \
    rm -r dist/ && \
    python3 setup.py sdist bdist_wheel && \
	python3 -m twine upload -u __token__ --repository testpypi dist/*
# password can be set in TWINE_PASSWORD. https://twine.readthedocs.io/en/latest/

.PHONY:package
package:venv
	source $(VENV_NAME)/bin/activate && \
    pip install --upgrade pip && \
    python setup.py install && \
    pip install --upgrade -r requirements.txt && \
    rm -r dist/ && \
    python3 setup.py sdist bdist_wheel && \
	python3 -m twine upload -u __token__ --repository pypi dist/*
# password can be set in TWINE_PASSWORD. https://twine.readthedocs.io/en/latest/

.PHONY:test
test:
	pytest tests

.PHONY:spark
spark:
	wget http://archive.apache.org/dist/spark/spark-$(SPARK_VER)/spark-$(SPARK_VER)-bin-hadoop$(HADOOP_VER).tgz && \
	mkdir -p /usr/local/opt/spark && \
	rm -r /usr/local/opt/spark/ && \
	mkdir -p /usr/local/opt/spark && \
	tar -zxvf spark-$(SPARK_VER)-bin-hadoop$(HADOOP_VER).tgz -C /usr/local/opt/spark && \
	cp -a /usr/local/opt/spark/spark-$(SPARK_VER)-bin-hadoop$(HADOOP_VER)/ /usr/local/opt/spark/ && \
	rm -r /usr/local/opt/spark/spark-$(SPARK_VER)-bin-hadoop$(HADOOP_VER)

.PHONY:dockerspark
dockerspark:
	docker run --name spark-master -h spark-master -e ENABLE_INIT_DAEMON=false -d bde2020/spark-master:3.0.0-hadoop3.2
	docker run --name spark-worker-1 --link spark-master:spark-master -e ENABLE_INIT_DAEMON=false -d bde2020/spark-worker:3.0.0-hadoop3.2
