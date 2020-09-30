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
	source $(VENV_NAME)/bin/activate && \
	pytest tests

.PHONY:sdkman
sdkman:
	sdk list java || \
	curl -s "https://get.sdkman.io" | bash

.PHONY:java
java:
	source "$(HOME)/.sdkman/bin/sdkman-init.sh" && \
	sdk install java 11.0.8.hs-adpt || echo "java installed"

.PHONY:scala
scala:
	source "$(HOME)/.sdkman/bin/sdkman-init.sh" && \
	sdk install scala 2.12.12 || echo "scala installed"

.PHONY:brew
brew:
	brew config || \
	curl -s "https://raw.githubusercontent.com/Homebrew/install/master/install.sh" | bash

.PHONY:wget
wget:
	brew install wget

.PHONY:docker
docker:
	# brew install docker  # this is different than cask install below
	# https://stackoverflow.com/questions/40523307/brew-install-docker-does-not-include-docker-engine
	brew cask install docker && \
	brew install bash-completion && \
	brew install docker-completion && \
	brew install docker-compose-completion && \
	brew install docker-machine-completion && \
	open /Applications/Docker.app

.PHONY:helm
helm:
	brew install kubernetes-helm

.PHONY:helmchart
helmchart:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm install my-release bitnami/spark

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
	docker run --name spark-master -p 8080:8080 -p 6066:6066 -p 7077:7077 -v /Users/imranqureshi/git/SparkPipelineFramework/tests:/Users/imranqureshi/git/SparkPipelineFramework/tests -h spark-master -e ENABLE_INIT_DAEMON=false -d bitnami/spark:3.0.1
	docker run --name spark-worker-1 -v /Users/imranqureshi/git/SparkPipelineFramework/tests:/Users/imranqureshi/git/SparkPipelineFramework/tests --link spark-master:spark-master -e ENABLE_INIT_DAEMON=false -d bitnami/spark:3.0.1

.PHONY:up
up:
	docker-compose up --detach && \
	sleep 5 && \
	open http://localhost:8080/

.PHONY:down
down:
	docker-compose down

.PHONY:installspark
installspark: sdkman java scala brew wget helm spark

.PHONY:firsttime
firsttime: installspark docker up devsetup proxies test

.PHONY:proxies
proxies:
	python3 spark_pipeline_framework/proxy_generator/generate_proxies.py
