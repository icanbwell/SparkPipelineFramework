![Build and Test](https://github.com/imranq2/SparkPipelineFramework/workflows/Build%20and%20Test/badge.svg)

![Upload Python Package](https://github.com/imranq2/SparkPipelineFramework/workflows/Upload%20Python%20Package/badge.svg)

[![Known Vulnerabilities](https://snyk.io/test/github/imranq2/SparkPipelineFramework/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/imranq2/SparkPipelineFramework?targetFile=requirements.txt)

# SparkPipelineFramework
SparkPipelineFramework implements a few design patterns to make it easier to create Spark applications that:
1. Separate data transformation logic from the pipeline execution code so you can compose pipelines by just stringing together transformers.  (Based on the SparkML Pipeline class but enhanced to work for both ML and non-ML transformations)
2. Enables running SQL transformations without writing any code
3. Enables versioning of transformations so different pipelines can use older or newer versions of each transformer.  This enables you to upgrade each pipeline at your own choice
4. Enables Autocomplete of transformations when creating pipelines (in PyCharm).
5. Implement many separation-of-concerns e.g., logging, performance monitoring, error reporting
6. Supports both non-ML, ML and mixed workloads
7. Has an additional library SparkPipelineFramework.AWS that makes running Spark pipelines in AWS easier
8. Has a sister library SparkPipelineFramework.Catalog that implements a data and ML model catalog so you can load and save data by catalog name instead of path and can manage different versions of the data

# PyPi Package
This code is available as a package to import into your project.
https://pypi.org/project/sparkpipelineframework/

# Using it in your project
(For an example project that uses SparkPipelineFramework, see https://github.com/imranq2/TestSparkPipelineFramework)
1. Add sparkpipelineframework package to your project requirements.txt/Pipefile
2. make init. (this will setup Spark, Docker (to run Spark) )
3. Create a folder called library in your project

## using Pycharm
You can run SparkPipelineFrame project from Pycharm
1. Add a new Docker Compose interpreter 
2. Choose `docker-compose.yml` for the configuration file
3. Choose `dev` for the Service 
4. Click OK and give Pycharm a couple of minutes to index the content of the docker container
5. Right click on the Test folder and click "Run 'pytest in tests'"

## To create a new pipeline
1. Create a class derived from ```FrameworkPipeline```
2. In your __init__ function set self.transformers to the list of transformers to run for this pipeline.
For example:
```
class MyPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super().__init__(parameters=parameters,
                                         progress_logger=progress_logger)
        self.transformers = self.create_steps([
            FrameworkCsvLoader(
                view="flights",
                path_to_csv=parameters["flights_path"]
            ),
            FeaturesCarriers(parameters=parameters).transformers,
        ])
```

## To Add a SQL transformation
1. Create a new folder and a .sql file in that folder. This folder should be in the library folder or any subfolder you choose under the library folder.
2. The name of the file is the name of the view that will be created/updated to store the result of your sql code e.g., carriers.sql means we will create/update a view called carriers with the results of your sql code.
2. Add your sql to it.  This can be any valid SparkSQL and can refer to any view created by the pipeline before this transformer is run.  For example:
```
SELECT carrier, crsarrtime FROM flights
```
2. Run the generate_proxies command as shown in the Generating Proxies section below
3. Now go to your Pipeline class __init__ and add to self.transformers.  Start the folder name and hit ctrl-space for PyCharm to autocomplete the name
4. That's it.  Your sql has been automaticaly wrapped in a Transformer which will do logging, monitor performance and do error checking


## To Add a Python transformation
1. Create a new folder and .py file in that folder.  This folder should be in the library folder or any subfolder you choose under the library folder.
2. In the .py file, create a new class and derive from Transformer (from spark ML).  Implement the _transform() function
For example:
```
from typing import Optional, Dict, Any

from pyspark import keyword_only
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.python_proxy_base import PythonProxyBase


class FeatureTransformer(PythonProxyBase):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self,
                 name: str = None,
                 parameters: Optional[Dict[str, Any]] = None,
                 progress_logger: Optional[ProgressLogger] = None,
                 verify_count_remains_same: bool = False
                 ) -> None:
        super(FeatureTransformer, self).__init__(name=name,
                                                 parameters=parameters,
                                                 progress_logger=progress_logger,
                                                 verify_count_remains_same=verify_count_remains_same)

    def _transform(self, df: DataFrame) -> DataFrame:
        pass
```
3. Run the generate_proxies command as shown in the Generating Proxies section below
3. Now go to your Pipeline class __init__ and add to self.transformers.  Start the folder name and hit ctrl-space for PyCharm to autocomplete the name

## To Add a Machine Learning training transformation (called ```fit``` or ```Estimator``` in SparkML lingo)
1. Create a new folder and .py file in that folder.  This folder should be in the library folder or any subfolder you choose under the library folder.
2. In the .py file, create a new class and derive from Estimator (from spark ML).  Implement the fit() function
3. Run the generate_proxies command as shown in the Generating Proxies section below
3. Now go to your Pipeline class __init__ and add to self.estimators.  Start the folder name and hit ctrl-space for PyCharm to autocomplete the name

## To Add a Machine Learning prediction transformation
1. Create a new folder and .py file in that folder.  This folder should be in the library folder or any subfolder you choose under the library folder.
2. In the .py file, create a new class and derive from Estimator (from spark ML).  Implement the _transform() function.  Note that that can be the same class you use for training and prediction.
3. Run the generate_proxies command as shown in the Generating Proxies section below
3. Now go to your Pipeline class __init__ and add to self.transformers.  Start the folder name and hit ctrl-space for PyCharm to autocomplete the name

## Including pipelines in other pipelines
Pipelines are fully composable so you can include one pipeline as a transformer in another pipeline.
For example:
```
class MyPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MyPipeline, self).__init__(parameters=parameters,
                                         progress_logger=progress_logger)
        self.transformers = self.create_steps([
            FrameworkCsvLoader(
                view="flights",
                path_to_csv=parameters["flights_path"]
            ),
            PipelineFoo(parameters=parameters).transformers,
            FeaturesCarriers(parameters=parameters).transformers,
        ])
```

## Generating Proxies
1. Run the following command to generate proxy classes.  These automatically wrap your sql with a Spark Transformer that can be included in a Pipeline with no additional code. 

```python3 spark_pipeline_framework/proxy_generator/generate_proxies.py```.  

You can also add this to your project Makefile to make it easier to run: 

```
.PHONY:proxies
proxies:
	python3 spark_pipeline_framework/proxy_generator/generate_proxies.py
  ```

# Testing
## Test a pipeline
A pipeline can be tested by providing test data in csv (or parquet), running the pipeline and then asserting for data in any view or dataframe.

```
from pathlib import Path
from typing import Dict, Any

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1
from library.features.carriers_python.v1.features_carriers_python_v1 import FeaturesCarriersPythonV1
from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_csv_loader import FrameworkCsvLoader
from spark_pipeline_framework.utilities.flattener import flatten


class MyPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MyPipeline, self).__init__(parameters=parameters,
                                         progress_logger=progress_logger)
        self.transformers = self.create_steps([
            FrameworkCsvLoader(
                view="flights",
                path_to_csv=parameters["flights_path"],
                progress_logger=progress_logger
            ),
            FeaturesCarriersV1(parameters=parameters, progress_logger=progress_logger).transformers,
            FeaturesCarriersPythonV1(parameters=parameters, progress_logger=progress_logger).transformers
        ])


def test_can_run_framework_pipeline(spark_session: SparkSession) -> None:
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath('./')
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema)

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    # Act
    parameters = {
        "flights_path": flights_path
    }

    with ProgressLogger() as progress_logger:
        pipeline: MyPipeline = MyPipeline(parameters=parameters, progress_logger=progress_logger)
        transformer = pipeline.fit(df)
        transformer.transform(df)

    # Assert
    result_df: DataFrame = spark_session.sql("SELECT * FROM flights2")
    result_df.show()

    assert result_df.count() > 0
```

## Testing a single Transformer directly
Each Transformer can be tested individually by setting up the data to pass into it (e.g., loading from csv) and then testing the result of running the transformer.
```
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from spark_pipeline_framework.transformers.framework_csv_loader import FrameworkCsvLoader
from spark_pipeline_framework.utilities.attr_dict import AttrDict

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1


def test_carriers_v1(spark_session: SparkSession):
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath('./')
    flights_path: str = f"file://{data_dir.joinpath('flights.csv')}"

    schema = StructType([])

    df: DataFrame = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD(), schema)

    spark_session.sql("DROP TABLE IF EXISTS default.flights")

    FrameworkCsvLoader(
        view="flights",
        path_to_csv=flights_path
    ).transform(dataset=df)

    parameters = {}

    FeaturesCarriersV1(parameters=parameters).transformers[0].transform(dataset=df)

    result_df: DataFrame = spark_session.sql("SELECT * FROM flights2")
    result_df.show()

    assert result_df.count() > 0
```

# Contributing
Run ```make init```
This will install Java, Scala, Spark and other packages

# Publishing a new package
1. Create a new release
3. The GitHub Action should automatically kick in and publish the package
4. You can see the status in the Actions tab
