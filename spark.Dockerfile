# Run stage
FROM imranq2/spark-py:3.0.46
USER root

ENV PYTHONPATH=/spf
ENV CLASSPATH=/spf/jars:$CLASSPATH

COPY Pipfile* /spf/
WORKDIR /spf

RUN df -h # for space monitoring
RUN pipenv sync --dev --system

# COPY ./jars/* /opt/bitnami/spark/jars/
# COPY ./conf/* /opt/bitnami/spark/conf/

#RUN cd / && /opt/spark/bin/spark-submit --master local[*] test.py

# run pre-commit once so it installs all the hooks and subsequent runs are fast
# RUN pre-commit install

# ENV SPARK_EXTRA_CLASSPATH

ENV AWS_DEFAULT_REGION=us-east-1
ENV AWS_REGION=us-east-1

ENV HADOOP_CONF_DIR=/opt/spark/conf

COPY . /spf

# USER 1001

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs
USER root
