FROM imranq2/helix.spark:3.3.0.33-precommit-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY ${project_root}/Pipfile* ./

ARG TARGETPLATFORM
RUN pipenv sync --dev --system

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD pre-commit run --all-files
