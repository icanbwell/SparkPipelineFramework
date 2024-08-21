from typing import Any, Dict, Optional

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType
from pyspark.errors import AnalysisException
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_list_catalog_table_names,
)


# move this class to SPF
class FrameworkKafkaReader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        kafka_brokers: str,
        topic: str,
        schema: StructType,
        starting_offset: int = -2,
        use_ssl: bool = True,
        previous_checkpoint_view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__()

        self.logger = get_logger(__name__)

        self.kafka_brokers: Param[str] = Param(self, "kafka_brokers", "")
        self._setDefault(kafka_brokers=kafka_brokers)

        self.topic: Param[str] = Param(self, "topic", "")
        self._setDefault(topic=topic)

        self.schema: Param[StructType] = Param(self, "schema", "")
        self._setDefault(schema=schema)

        self.starting_offset: Param[int] = Param(self, "starting_offset", "")
        self._setDefault(starting_offset=starting_offset)

        self.use_ssl: Param[bool] = Param(self, "use_ssl", "")
        self._setDefault(use_ssl=use_ssl)

        self.previous_checkpoint_view: Param[Optional[str]] = Param(
            self, "previous_checkpoint_view", ""
        )
        self._setDefault(previous_checkpoint_view=previous_checkpoint_view)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        kafka_brokers: str = self.getKafkaBrokers()
        topic: str = self.getTopic()
        starting_offset: int = self.getStartingOffset()
        use_ssl: bool = self.getUseSsl()
        schema: StructType = self.getSchema()
        name: Optional[str] = self.getName()
        previous_checkpoint_view: Optional[str] = self.getPreviousCheckpointView()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(
            name=f"{name or topic}_kafka_reader", progress_logger=progress_logger
        ):
            try:
                if previous_checkpoint_view in [
                    t for t in spark_list_catalog_table_names(df.sparkSession)
                ]:
                    last_offset = (
                        df.sparkSession.table(previous_checkpoint_view)
                        .groupBy()
                        .max("offset")
                        .collect()[0]
                        .asDict()["max(offset)"]
                    )
                    if last_offset:
                        starting_offset = last_offset + 1

                security_protocol = "PLAINTEXT"
                if use_ssl:
                    security_protocol = "SSL"

                starting_offset_text = f"""{{"{topic}":{{"0":{starting_offset}}}}}"""
                df = (
                    df.sparkSession.read.format("kafka")
                    .option("kafka.bootstrap.servers", kafka_brokers)
                    .option("kafka.security.protocol", security_protocol)
                    .option("subscribe", f"{topic}")
                    .option("startingOffsets", starting_offset_text)
                    .load()
                )
                df = df.selectExpr("*", "CAST(value AS STRING) as event")
                df = df.withColumn("event", from_json(df.event, schema))
                df.createOrReplaceTempView(topic)
                if len(df.head(1)) == 0 and previous_checkpoint_view:
                    df.sparkSession.table(
                        previous_checkpoint_view
                    ).createOrReplaceTempView(f"{topic}_watermark")
                else:
                    df.createOrReplaceTempView(f"{topic}_watermark")
            except AnalysisException as e:
                self.logger.error(
                    f"Failed to read from kafka topic: {topic} on {kafka_brokers}"
                )
                raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getKafkaBrokers(self) -> str:
        return self.getOrDefault(self.kafka_brokers)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getTopic(self) -> str:
        return self.getOrDefault(self.topic)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStartingOffset(self) -> int:
        return self.getOrDefault(self.starting_offset)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getUseSsl(self) -> bool:
        return self.getOrDefault(self.use_ssl)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return self.getOrDefault(self.schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPreviousCheckpointView(self) -> Optional[str]:
        return self.getOrDefault(self.previous_checkpoint_view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return self.getOrDefault(self.name) or self.getOrDefault(self.topic)
