LANG=en_US.utf-8

export LANG

Pipfile.lock: Pipfile
	docker-compose run --rm --name spark_pipeline_framework dev sh -c "rm -f Pipfile.lock && pipenv lock --dev"

.PHONY:devdocker
devdocker: ## Builds the docker for dev
	docker-compose build

.PHONY:shell
shell:devdocker ## Brings up the bash shell in dev docker
	docker-compose run --rm --name helix_shell dev /bin/bash

.PHONY:init
init: devdocker up setup-pre-commit  ## Initializes the local developer environment

.PHONY: up
up: Pipfile.lock
	docker-compose up --build -d --remove-orphans && \
	echo "\nwaiting for Mongo server to become healthy" && \
	while [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework_mongo_1`" != "healthy" ] && [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework_mongo_1`" != "unhealthy" ] && [ "`docker inspect --format {{.State.Status}} sparkpipelineframework_mongo_1`" != "restarting" ]; do printf "." && sleep 2; done && \
	if [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework_mongo_1`" != "healthy" ]; then docker ps && docker logs sparkpipelineframework_mongo_1 && printf "========== ERROR: sparkpipelineframework_mongo_1 did not start. Run docker logs sparkpipelineframework_mongo_1 =========\n" && exit 1; fi && \
	echo "\nwaiting for Fhir server to become healthy" && \
	while [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework_fhir_1`" != "healthy" ] && [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework_fhir_1`" != "unhealthy" ] && [ "`docker inspect --format {{.State.Status}} sparkpipelineframework_fhir_1`" != "restarting" ]; do printf "." && sleep 2; done && \
	if [ "`docker inspect --format {{.State.Health.Status}} sparkpipelineframework_fhir_1`" != "healthy" ]; then docker ps && docker logs sparkpipelineframework_fhir_1 && printf "========== ERROR: sparkpipelineframework_fhir_1 did not start. Run docker logs sparkpipelineframework_fhir_1 =========\n" && exit 1; fi
	@echo MockServer dashboard: http://localhost:1080/mockserver/dashboard
	@echo Spark dashboard: http://localhost:8080/
	@echo Fhir server dashboard http://localhost:3000/

.PHONY: down
down:
	docker-compose down --remove-orphans && \
	docker volume prune --filter label=mlflow -f

.PHONY:clean-pre-commit
clean-pre-commit: ## removes pre-commit hook
	rm -f .git/hooks/pre-commit

.PHONY:setup-pre-commit
setup-pre-commit: Pipfile.lock
	cp ./pre-commit-hook ./.git/hooks/pre-commit

.PHONY:run-pre-commit
run-pre-commit: setup-pre-commit
	./.git/hooks/pre-commit

.PHONY:update
update: down Pipfile.lock setup-pre-commit  ## Updates all the packages using Pipfile
	docker-compose run --rm --name spf_pipenv dev pipenv sync --dev && \
	make devdocker && \
	make pipenv-setup

.PHONY:tests
tests:
	docker-compose run --rm --name spf_tests dev pytest tests spark_pipeline_framework

.PHONY:proxies
proxies:
	docker-compose run --rm --name spf_proxies dev python /SparkpipelineFramework/spark_pipeline_framework/proxy_generator/generate_proxies.py

.PHONY: sphinx-html
sphinx-html:
	docker-compose run --rm --name spark_pipeline_framework dev make -C docsrc html
	@echo "copy html to docs... why? https://github.com/sphinx-doc/sphinx/issues/3382#issuecomment-470772316"
	@rm -rf docs/*
	@touch docs/.nojekyll
	cp -a docsrc/_build/html/. docs

.PHONY:pipenv-setup
pipenv-setup:devdocker ## Brings up the bash shell in dev docker
	docker-compose run --rm --name spark_pipeline_framework dev pipenv-setup sync --pipfile
