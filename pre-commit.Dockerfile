FROM imranq2/helix.spark:3.5.1.11-precommit-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY Pipfile* ./

ARG TARGETPLATFORM
RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary"

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD ["pre-commit", "run", "--all-files"]
