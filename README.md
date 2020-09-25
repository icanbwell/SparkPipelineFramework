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
1. Add sparkpipelineframework package to your project requirements.txt
2. Create a folder called library in your project

## To create a new pipeline
1. Create a class derived from ```FrameworkPipeline```
2. In your __init__ funtion set self.transformers to the list of transformers to run for this pipeline.
For example:
```
class MyPipeline(FrameworkPipeline):
    def __init__(self, parameters: AttrDict, progress_logger: ProgressLogger):
        super(MyPipeline, self).__init__(parameters=parameters,
                                         progress_logger=progress_logger)
        self.transformers = flatten([
            [
                FrameworkCsvLoader(
                    view="flights",
                    path_to_csv=parameters["flights_path"]
                )
            ],
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
class MyPythonTransformer(Transformer):
	def _transform(self, df: DataFrame) -> DataFrame:
		# read parameters and do your stuff here.  You can either create/update a view or just update the passed in dataframe.
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
    def __init__(self, parameters: AttrDict, progress_logger: ProgressLogger):
        super(MyPipeline, self).__init__(parameters=parameters,
                                         progress_logger=progress_logger)
        self.transformers = flatten([
            [
                FrameworkCsvLoader(
                    view="flights",
                    path_to_csv=parameters["flights_path"]
                )
            ],
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

# Contributing
Run ```make firstime```
This will install Java, Scala, Spark and other packages
