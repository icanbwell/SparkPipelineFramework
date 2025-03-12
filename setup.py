from setuptools import setup, find_packages
from os import path, getcwd

# from https://packaging.python.org/tutorials/packaging-projects/

# noinspection SpellCheckingInspection
package_name = "sparkpipelineframework"

with open("README.md", "r") as fh:
    long_description = fh.read()

try:
    with open(path.join(getcwd(), "VERSION")) as version_file:
        version = version_file.read().strip()
except IOError:
    raise


# classifiers list is here: https://pypi.org/classifiers/

# create the package setup
setup(
    name=package_name,
    version=version,
    author="Imran Qureshi",
    author_email="imranq2@hotmail.com",
    description="Framework for simpler Spark Pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/icanbwell/SparkPipelineFramework",
    packages=find_packages(
        exclude=[
            "*/test",
            "*/test/*",
            "*tests/*",
            "*library/*",
            "*library",
            "docs",
            "docs/*",
            "docsrc",
            "docsrc/*",
            "keycloak-config",
            "keycloak-config/*",
            ".gihub",
            ".github/*",
        ]
    ),
    install_requires=[
        "protobuf>=3",
        "pyspark==3.5.1",
        "pyarrow>=17.0.0",
        "delta-spark==3.2.0",
        "sparkautomapper>=3.0.1",
        "pymysql>=1.1.1",
        "furl>=2.1.3",
        "requests>=2.32.3",
        "boto3>=1.34.140",
        "chardet",
        "slack-sdk>=3.22.0",
        "smart-open[s3]>=6.3.0",
        "mlflow-skinny==2.17.2",
        "sqlalchemy>=1.4.37",
        "sqlparse>=0.5.3",
        "bounded-pool-executor>=0.0.3",
        "fastjsonschema>=2.18.0",
        "helix.fhir.client.sdk>=3.0.29",
        "opensearch-py[async]>=2.6.0",
        "pyathena>2.14.0",
        "spark-nlp>=4.2.3",
        "pymongo>=4.10.1",
        "pandas>=2",
        "structlog>=23.1.0",
        "usaddress>=0.5.10",
        "usaddress-scourgify>=0.6.0",
        "aiohttp>=3.10.11",
        "pydantic>=2.8.2",
        "motor[snappy,zstd]>=3.5.1",
        "dataclasses-json>=0.6.7",
        "opentelemetry-api>=1.30.0",
        "opentelemetry-sdk>=1.30.0",
        "opentelemetry-exporter-otlp>=1.30.0",
        "opentelemetry-instrumentation-system-metrics>=0.51b0",
        "opentelemetry-instrumentation-logging>=0.51b0",
        "opentelemetry-instrumentation-pymysql>=0.51b0",
        "opentelemetry-instrumentation-pymongo>=0.51b0",
        "opentelemetry-instrumentation-elasticsearch>=0.51b0",
        "opentelemetry-instrumentation-boto>=0.51b0",
        "opentelemetry-instrumentation-botocore>=0.51b0",
        "opentelemetry-instrumentation-httpx>=0.51b0",
        "opentelemetry-instrumentation-asyncio>=0.51b0",
        "opentelemetry-instrumentation-aiohttp-client>=0.51b0",
        "opentelemetry-instrumentation-confluent-kafka>=0.51b0",
        "opentelemetry-instrumentation-requests>=0.51b0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    dependency_links=[],
    include_package_data=True,
    zip_safe=False,
    package_data={"spark_pipeline_framework": ["py.typed"]},
)
