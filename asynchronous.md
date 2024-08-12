# Asynchronous Processing

## Introduction
Asynchronous processing is a form of parallel processing that allows a system to handle multiple requests at the same time. This is particularly useful when a system needs to wait on other systems to respond, such as reading from a file, accessing a web server (e.g. FHIR or Elasticsearch) or making a network request.

Instead of blocking the current process, we can "await" the response from the other system and continue processing other requests. 

This is the `async` and `await` pattern in Python (and other programming languages).  The functions are defined with an "async" prefix and then we use "await" on calls to external systems.

## How use Asynchronous Processing with Spark
Apache Spark is a distributed computing system that is designed to process large datasets quickly. 
Each partition of a data frame can be processed in parallel on different nodes in a cluster.  

However if we are not using async calls then each worker node can get blocked waiting for the external system to respond for the call we make for that partition.

Instead, what we want is that each worker can make multiple async calls at the same time and then once all the async calls have returned then that partition is finished and the worker node can work on a different partition.


## Using async and await with Spark
In this framework, we provide a few helper classes that allow you to easily use async functions with Spark data frames.  
These functions can be used with the `mapInPandas()` function in Spark or as Pandas UDF (User Defined Functions) in the `withColumn()` or `select()` function.

### Case 1: When you want to read a whole data frame and return a different data frame
Examples of this include when you read FHIR resources from a source dataframe, send those to the FHIR server and return a data frame that contains the responses from the FHIR server.
In this case your goal is to create a new data frame that has the same number of rows as the input data frame.

For this case, you can use the `AsyncPandasDataFrameUDF` class like so:
```python
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_dataframe_udf import (
    AsyncPandasDataFrameUDF,
)
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasDataFrameBatchFunction,
)
from typing import (
    List,
    Dict,
    Any,
    Optional,
    AsyncGenerator,
    cast,
    Iterable,
    Tuple,
    Generator,
)
import dataclasses
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@dataclasses.dataclass
class MyParameters:
    log_level: str
    
    

def my_code():
    async def test_async(
        *,
        partition_index: int,
        chunk_index: int,
        chunk_input_range: range,
        input_values: List[Dict[str, Any]],
        parameters: Optional[MyParameters],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        # your async code here
        # yield a dict for each row in the input_values list
        yield {
            "name": "test"
        }
    
        output_schema = StructType(
            [
                StructField("name", StringType(), True),
            ]
        )
        result_df: DataFrame = source_df.mapInPandas(
            AsyncPandasDataFrameUDF(
                parameters=MyParameters(log_level="DEBUG"),
                async_func=cast(
                    HandlePandasDataFrameBatchFunction[MyParameters], test_async
                ),
                batch_size=2,
        ).get_pandas_udf(),
        schema=output_schema,
    )
```

### Case 2: When you want to read a single column (or set of columns) and want to append (or replace) a column in the same dataframe
Example of this include reading the raw_address column of every row, calling a service to standardize the address and then adding the standardized address as a new column called standardized_address.

In this case, your goal is to add columns to an existing data frame and not replace the whole dataframe.

There are four kinds of column transformations:
1. Struct column to struct column. Use `AsyncPandasStructColumnToStructColumnUDF` class.
2. Struct column to scalar column. Use `AsyncPandasStructColumnToScalarColumnUDF` class.
3. Scalar column to struct column. Use `AsyncPandasScalarColumnToStructColumnUDF` class.
4. Scalar column to scalar column. Use `AsyncPandasScalarColumnToScalarColumnUDF` class.

For example for scalar column to scalar column transformation, you can use the `AsyncPandasScalarColumnToScalarColumnUDF` class like so:
```python
from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_scalar_column_to_scalar_udf import (
    AsyncPandasScalarColumnToScalarColumnUDF,
)
from pyspark.sql import SparkSession, DataFrame
from spark_pipeline_framework.utilities.async_pandas_udf.v1.function_types import (
    HandlePandasScalarToScalarBatchFunction,
)   
from typing import (
    List,
    Dict,
    Any,
    Optional,
    AsyncGenerator,
    cast,
)
import dataclasses
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@dataclasses.dataclass
class MyParameters:
    log_level: str

def my_code():
    async def test_async(
        *,
        partition_index: int,
        chunk_index: int,
        chunk_input_range: range,
        input_values: List[Dict[str, Any]],
        parameters: Optional[MyParameters],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        # your async code here
        # yield a dict for each row in the input_values list
        yield {}
    
        output_schema = StructType(
            [
                # your output schema here
            ]
        )
        source_df: DataFrame = spark.createDataFrame(
            [
                # your data here
            ],
            schema=StructType(
                [
                    StructField("raw_address", StringType(), True),
                ]
            ),
        )
        result_df: DataFrame = df.withColumn(
            colName="processed_name",
            col=AsyncPandasScalarColumnToScalarColumnUDF(
                async_func=cast(
                    HandlePandasScalarToScalarBatchFunction[MyParameters],
                    test_async,
                ),
                parameters=MyParameters(log_level="DEBUG"),
                batch_size=2,
            ).get_pandas_udf(return_type=StringType())(source_df["name"]),
    )
```

# Logging
The partition_index and chunk_index are passed to the async function so that you can log them. This can be useful for debugging purposes. The log level is passed as a parameter to the async function so that you can control the log level.

In addition, you can use the SparkPartitionInformation class to get additional information about the partition, such as the number of rows in the partition and the partition index.

```python
from spark_pipeline_framework.utilities.spark_partition_information.v1.spark_partition_information import (
    SparkPartitionInformation,
)
from typing import (
    List,
    Dict,
    Any,
    Optional,
    AsyncGenerator,
    cast,
    Iterable,
    Tuple,
    Generator,
)

async def test_async(
        *,
        partition_index: int,
        chunk_index: int,
        chunk_input_range: range,
        input_values: List[Dict[str, Any]],
        parameters: Optional[MyParameters],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        spark_partition_information: SparkPartitionInformation = (
            SparkPartitionInformation.from_current_task_context(
                chunk_index=chunk_index,
            )
        )
        print(str(spark_partition_information))
        # your async code here
        # yield a dict for each row in the input_values list
        yield {}
        

```

# Memory usage
Typically, when you transform a data frame in Pyton code, Spark has to serialize the data frame and send it to the Python worker.  And then it has to serialize the result and send it back to the JVM.

This can be memory intensive. 

However, this framework uses Arrow (a columnar memory framework) so the data is shared between JVM and Python without needing to serialize and deserialize between Python and JVM.

# Use asyncio drivers for external systems
To make this work you need to use an async driver for the external system you are calling.  The synchronous drivers will block the worker node and prevent it from making other async calls.

For example for HTTP calls, you can use `aiohttp` library and for elastic search you can use `opensearch-py` library.

You can find a list of async drivers here: https://github.com/timofurrer/awesome-asyncio

# Controlling how big of a batch to send to the async function
Set the `batch_size` parameter in the `AsyncPandasDataFrameUDF` or `AsyncPandasScalarColumnToScalarColumnUDF` class to control how many rows to send to the async function at a time.  

The partition will be divided into chunks of `batch_size` rows and each chunk will be sent to the async function at the same time.

Hence you should set the `partition_size` to be as large as can fit in memory and then let this divide into batches of `batch_size` to send to async function at one time.

This will be most efficient since you'll have multiple async calls (one for each batch) waiting in each partition.


# Summary
By using these classes you can easily use async functions with Spark data frames. This can be useful when you need to perform tasks that are time-consuming, such as reading from a file, accessing a web server (e.g. FHIR or Elasticsearch) or making a network request. By using asynchronous processing, a system can handle multiple requests at the same time, which can improve performance and reduce latency.
