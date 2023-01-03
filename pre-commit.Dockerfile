FROM imranq2/helix.spark:3.3.0.20-precommit-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY ${project_root}/Pipfile* ./

RUN pipenv sync --dev --system

WORKDIR /sourcecode
CMD pre-commit run --all-files
