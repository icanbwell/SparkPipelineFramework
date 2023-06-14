FROM imranq2/helix.spark:3.3.0.22-precommit-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY ${project_root}/Pipfile* ./

#RUN pipenv sync --dev --system

ARG TARGETPLATFORM
RUN echo "-------------"
RUN echo "-=---$TARGETPLATFORM----"
RUN echo "-------------"
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; \
    then pipenv sync --dev --system && echo "#########"; \
    else rm -rf Pipfile.lock && pipenv lock && pipenv sync --dev --system && echo "11111111111"; fi

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD pre-commit run --all-files
