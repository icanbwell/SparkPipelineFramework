LANG=en_US.utf-8

export LANG

# The helix.spark base images used by spark.Dockerfile and pre-commit.Dockerfile live in
# the private services ECR (CIE-8032), so docker must be logged in before any build.
# AWS_SERVICES_PROFILE is empty by default so that CI can use the ambient credentials
# supplied by aws-actions/configure-aws-credentials.  Local developers should run
# `aws sso login --profile services` and then `make AWS_SERVICES_PROFILE=services <target>`
# (or simply `export AWS_PROFILE=services`).
AWS_SERVICES_PROFILE ?=
AWS_SERVICES_REGION ?= us-east-1
AWS_SERVICES_REGISTRY ?= 856965016623.dkr.ecr.us-east-1.amazonaws.com

## Logs docker in to the private ECR repo hosting the helix.spark base image.
.PHONY: ecr-login
ecr-login:
	aws ecr get-login-password --region $(AWS_SERVICES_REGION) \
	  $(if $(AWS_SERVICES_PROFILE),--profile $(AWS_SERVICES_PROFILE),) \
	  | docker login --username AWS --password-stdin $(AWS_SERVICES_REGISTRY)

.PHONY: Pipfile.lock
Pipfile.lock: build
	docker compose run --rm --name spftest dev /bin/bash -c "rm -f Pipfile.lock && pipenv lock --dev"

.PHONY: install_types
install_types:
	docker compose run --rm --name spark_pipeline_framework dev pipenv run mypy --install-types --non-interactive

.PHONY:devdocker
devdocker: ecr-login ## Builds the docker for dev
	docker compose build

.PHONY:shell
shell:devdocker ## Brings up the bash shell in dev docker
	docker compose run --rm --name helix_shell dev /bin/bash

.PHONY:init
init: devdocker up setup-pre-commit  ## Initializes the local developer environment

.PHONY: up
up: ecr-login
	docker compose up --build -d --remove-orphans && \
	echo "\nwaiting for Mongo server to become healthy" && \
	while [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework-mongo-1`" != "healthy" ] && [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework-mongo-1`" != "unhealthy" ] && [ "`docker inspect --format {{.State.Status}} sparkpipelineframework-mongo-1`" != "restarting" ]; do printf "." && sleep 2; done && \
	if [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework-mongo-1`" != "healthy" ]; then docker ps && docker logs sparkpipelineframework-mongo-1 && printf "========== ERROR: sparkpipelineframework-mongo-1 did not start. Run docker logs sparkpipelineframework-mongo-1 =========\n" && exit 1; fi && \
	echo "\nwaiting for Fhir server to become healthy" && \
	while [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework-fhir-1`" != "healthy" ] && [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework-fhir-1`" != "unhealthy" ] && [ "`docker inspect --format {{.State.Status}} sparkpipelineframework-fhir-1`" != "restarting" ]; do printf "." && sleep 2; done && \
	if [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework-fhir-1`" != "healthy" ]; then docker ps && docker logs sparkpipelineframework-fhir-1 && printf "========== ERROR: sparkpipelineframework-fhir-1 did not start. Run docker logs sparkpipelineframework-fhir-1 =========\n" && exit 1; fi
	@echo MockServer dashboard: http://localhost:1080/mockserver/dashboard
	@echo Spark dashboard: http://localhost:8080/
	@echo Fhir server dashboard http://localhost:3000/
	@echo Keycloak OAuth dashboard http://admin:password@localhost:8080/

.PHONY: down
down:
	docker compose down --remove-orphans && \
	docker system prune -f && \
	docker volume prune --filter label=mlflow -f

.PHONY:clean-pre-commit
clean-pre-commit: ## removes pre-commit hook
	rm -f .git/hooks/pre-commit

.PHONY:setup-pre-commit
setup-pre-commit:
	cp ./pre-commit-hook ./.git/hooks/pre-commit

.PHONY:run-pre-commit
run-pre-commit: ecr-login setup-pre-commit
	./.git/hooks/pre-commit

.PHONY:update
update: down Pipfile.lock setup-pre-commit  ## Updates all the packages using Pipfile
	make devdocker && \
	make pipenv-setup && \
	make up

.PHONY:tests
tests:
	docker compose run --rm --name spf_tests dev pytest tests spark_pipeline_framework

.PHONY:proxies
proxies:
	docker compose run --rm --name spf_proxies dev python /SparkpipelineFramework/spark_pipeline_framework/proxy_generator/generate_proxies.py

.PHONY: sphinx-html
sphinx-html:
	docker compose run --rm --name spark_pipeline_framework dev make -C docsrc html
	@echo "copy html to docs... why? https://github.com/sphinx-doc/sphinx/issues/3382#issuecomment-470772316"
	@rm -rf docs/*
	@touch docs/.nojekyll
	cp -a docsrc/_build/html/. docs

.PHONY:pipenv-setup
pipenv-setup:devdocker ## Run pipenv-setup to update setup.py with latest dependencies
	docker compose run --rm --name spark_pipeline_framework dev sh -c "pipenv run pipenv install --skip-lock --categories \"pipenvsetup\" && pipenv run pipenv-setup sync --pipfile" && \
	make run-pre-commit

.PHONY: clean_data
clean_data: down ## Cleans all the local docker setup
ifneq ($(shell docker volume ls | grep "sparkpipelineframework"| awk '{print $$2}'),)
	docker volume ls | grep "sparkpipelineframework" | awk '{print $$2}' | xargs docker volume rm
endif

.PHONY:show_dependency_graph
show_dependency_graph:
	docker compose run --rm --name spark_pipeline_framework dev sh -c "pipenv install --skip-lock -d && pipenv graph --reverse"
	docker compose run --rm --name spark_pipeline_framework dev sh -c "pipenv install -d && pipenv graph"

.PHONY:build
build: ecr-login ## Builds the docker for dev
	docker compose build --progress=plain --parallel

.PHONY:clean
clean: down
	find . -type d -name "__pycache__" | xargs rm -r
	find . -type d -name "metastore_db" | xargs rm -r
	find . -type f -name "derby.log" | xargs rm -r
	find . -type d -name "temp" | xargs rm -r
