FROM imranq2/helix.spark:3.5.1.1-slim
# https://github.com/icanbwell/helix.spark
USER root

ENV PYTHONPATH=/SparkpipelineFramework
ENV CLASSPATH=/SparkpipelineFramework/jars:$CLASSPATH

# remove the older version of entrypoints with apt-get because that is how it was installed
RUN apt-get remove python3-entrypoints -y

COPY Pipfile* /SparkpipelineFramework/
WORKDIR /SparkpipelineFramework

#RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary" && pipenv run pip install pyspark==3.5.1
#COPY ./jars/* /opt/spark/jars/
#COPY ./conf/* /opt/spark/conf/
# run this to install any needed jars by Spark
COPY ./test.py ./
RUN /opt/spark/bin/spark-submit --master local[*] test.py

ARG TARGETPLATFORM
RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary"

USER root
# install python 3.12 - it's not available in normal ubuntu repositories
# https://github.com/deadsnakes/issues/issues/53
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys F23C5A6CF475977595C89F51BA6932366A755776 && \
    echo "deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu/ jammy main" | tee /etc/apt/sources.list.d/deadsnakes-ubuntu-ppa-lunar.list && \
    apt-get update && apt-get install -y python3.12 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

COPY . /SparkpipelineFramework

# override entrypoint to remove extra logging
RUN mv /opt/minimal_entrypoint.sh /opt/entrypoint.sh

# run pre-commit once so it installs all the hooks and subsequent runs are fast
# RUN pre-commit install
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs

# Run as non-root user
# https://spark.apache.org/docs/latest/running-on-kubernetes.html#user-identity
USER 185

# RUN spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.2
# RUN spark-shell --jar spark-nlp-assembly-4.2.2
