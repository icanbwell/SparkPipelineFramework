import setuptools

# from https://packaging.python.org/tutorials/packaging-projects/

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sparkpipelineframework",
    version="0.0.1",
    author="Imran Qureshi",
    author_email="imranq2@hotmail.com",
    description="Framework for simpler Spark Pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/imranq2/SparkPipelineFramework",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache 2.0 License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)