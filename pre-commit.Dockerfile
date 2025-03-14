FROM imranq2/helix.spark:3.5.1.11-precommit-slim

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
