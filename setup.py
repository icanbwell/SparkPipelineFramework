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
        "protobuf>=3",
        "pyspark==3.3.0",
        "pyarrow>=17.0.0",
        "delta-spark==2.3.0",
        "sparkautomapper>=2.0.7",
        "pymysql>=1.0.3",
        "furl>=2.1.3",
        "requests>=2.31.0",
        "boto3>=1.34.140",
        "chardet",
        "slack-sdk>=3.22.0",
        "smart-open[s3]>=6.3.0",
        "mlflow-skinny>=2.15.0",
        "sqlalchemy>=1.4.37",
        "alembic>=1.10.0",
        "sqlparse>=0.4.4",
        "bounded-pool-executor>=0.0.3",
        "fastjsonschema>=2.18.0",
        "helix.fhir.client.sdk>=2.0.8",
        "opensearch-py>=1.1.0",
        "pyathena>2.14.0",
        "spark-nlp>=4.2.3",
        "pymongo>=4.8.0",
        "more-itertools>=9.1.0",
        "pandas>=2.2.2",
        "numexpr>=2.8.4",
        "bottleneck>=1.3.6",
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
