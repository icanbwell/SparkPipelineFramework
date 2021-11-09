FROM imranq2/spark_python:0.1.29
# https://github.com/imranq2/docker.spark_python
USER root

ENV PYTHONPATH=/spf
ENV CLASSPATH=/spf/jars:$CLASSPATH

COPY Pipfile* /spf/
WORKDIR /spf

RUN df -h # for space monitoring
RUN pipenv sync --dev --system

# COPY ./jars/* /opt/bitnami/spark/jars/
# COPY ./conf/* /opt/bitnami/spark/conf/

COPY . /spf

# run pre-commit once so it installs all the hooks and subsequent runs are fast
# RUN pre-commit install
RUN df -h # for space monitoring
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs
# USER 1001
