FROM imranq2/helix.spark:3.3.0.31-precommit-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY ${project_root}/Pipfile* ./

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; \
    then pipenv sync --dev --system; \
    else rm -rf Pipfile.lock && pipenv lock && pipenv sync --dev --system; fi

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD pre-commit run --all-files
