FROM 856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark:3.5.1.11-precommit-slim
# Pulled from the private services ECR per CIE-8032.  Registry only - the tag is unchanged,
# so the image is byte-identical to what was used before.
# Building/pulling requires `make ecr-login` locally (see Makefile).
#
# NOTE: this does NOT clear Aikido CUSTOM-RULE-2300 ("not sourced from root.io ECR mirror"),
# which accepts only .../root-mirror/*.  Rebasing this file onto
# `856965016623.dkr.ecr.us-east-1.amazonaws.com/root-mirror/python:3.12-slim` was measured
# (via Aikido's own engine) to clear both 559 and 2300, but is deliberately NOT done here:
# that tag is unverified (no root-mirror/*-slim is used anywhere in the org) whereas this
# tag is proven present in the ECR.  Deferred to the base-image owners.

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY Pipfile* ./

ARG TARGETPLATFORM
RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary"

# Add and switch to a non-root user
# /reports is being used in workflow to store pytest results
RUN groupadd -g 1001 nonrootgroup && \
    useradd -m -u 1001 -g 1001 -s /bin/bash nonrootuser && \
    mkdir -p /reports && \
    chown -R 1001:1001 /reports

USER nonrootuser

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD ["pre-commit", "run", "--all-files"]
