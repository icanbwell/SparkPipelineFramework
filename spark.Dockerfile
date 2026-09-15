FROM icanbwell/helix.spark:4.2.0.0-slim
# https://github.com/icanbwell/helix.spark
USER root

ENV PYTHONPATH=/SparkpipelineFramework
ENV CLASSPATH=/SparkpipelineFramework/jars:$CLASSPATH

# remove the older version of entrypoints with apt-get because that is how it was installed
# (skip if package not present — helix.spark 4.x images no longer include it)
RUN apt-get remove python3-entrypoints -y 2>/dev/null || true

# remove python3.10 stuff if present (not in helix.spark 4.x images)
RUN rm -rf /usr/local/lib/python3.10 2>/dev/null || true

COPY Pipfile* /SparkpipelineFramework/
WORKDIR /SparkpipelineFramework

#COPY ./jars/* /opt/spark/jars/
#COPY ./conf/* /opt/spark/conf/
# run this to install any needed jars by Spark
COPY ./test.py ./
RUN /opt/spark/bin/spark-submit --master local[*] test.py

ARG TARGETPLATFORM
RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary"

COPY . /SparkpipelineFramework

# override entrypoint to remove extra logging
RUN mv /opt/minimal_entrypoint.sh /opt/entrypoint.sh

# run pre-commit once so it installs all the hooks and subsequent runs are fast
# RUN pre-commit install
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs

# Run as non-root user
# Change ownership of the directory and its subdirectories
RUN chown -R spark:spark /SparkpipelineFramework

# Set permissions to allow writing (read, write, execute for owner)
RUN chmod -R 755 /SparkpipelineFramework
# https://spark.apache.org/docs/latest/running-on-kubernetes.html#user-identity
USER spark
