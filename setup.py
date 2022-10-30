# noinspection Mypy
from typing import Any

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


def fix_setuptools() -> None:
    """Work around bugs in setuptools.

    Some versions of setuptools are broken and raise SandboxViolation for normal
    operations in a virtualenv. We therefore disable the sandbox to avoid these
    issues.
    """
    try:
        from setuptools.sandbox import DirectorySandbox

        # noinspection PyUnusedLocal
        def violation(operation: Any, *args: Any, **_: Any) -> None:
            print("SandboxViolation: %s" % (args,))

        DirectorySandbox._violation = violation
    except ImportError:
        pass


# Fix bugs in setuptools.
fix_setuptools()

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
    url="https://github.com/imranq2/SparkPipelineFramework",
    packages=find_packages(),
    install_requires=[
        "logger>=1.4",
        "protobuf>=3.20.*",
        "pyspark>=3.1.1",
        "sparkautomapper>=1.0.19",
        "pymysql==1.0.2",
        "furl>=2.1.3",
        "requests>=2.27.1",
        "boto3>=1.21.32",
        "types-requests>=0.1.11",
        "types-pymysql>=0.1.5",
        "chardet>=4.0.0",
        "slack-sdk>=3.15.2",
        "smart-open[s3]>=5.2.1",
        "mlflow-skinny>=1.26.0",
        "sqlalchemy>=1.4.37",
        "alembic>=1.8.0",
        "sqlparse>=0.4.2",
        "bounded-pool-executor>=0.0.3",
        "fastjsonschema",
        "helix.fhir.client.sdk>=1.0.27",
        "opensearch-py",
        "pyathena",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    dependency_links=[],
    include_package_data=True,
    zip_safe=False,
    package_data={"spark_pipeline_framework": ["py.typed"]},
)
