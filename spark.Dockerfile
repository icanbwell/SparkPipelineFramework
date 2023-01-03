FROM imranq2/helix.spark:3.3.0.20-slim
# https://github.com/icanbwell/helix.spark
USER root

ENV PYTHONPATH=/spf
ENV CLASSPATH=/spf/jars:$CLASSPATH

# remove the older version of entrypoints with apt-get because that is how it was installed
RUN apt-get remove python3-entrypoints -y

COPY Pipfile* /spf/
WORKDIR /spf

RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary" && pipenv run pip install pyspark==3.3.0

#COPY ./jars/* /opt/spark/jars/
#COPY ./conf/* /opt/spark/conf/
# run this to install any needed jars by Spark
COPY ./test.py ./
RUN /opt/spark/bin/spark-submit --master local[*] test.py

COPY . /spf

# override entrypoint to remove extra logging
RUN mv /opt/minimal_entrypoint.sh /opt/entrypoint.sh

# run pre-commit once so it installs all the hooks and subsequent runs are fast
# RUN pre-commit install
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs
# USER 1001

# RUN spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.2
# RUN spark-shell --jar spark-nlp-assembly-4.2.2
