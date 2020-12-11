from typing import Optional, Any, Dict, cast

from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json
from pyspark.sql.utils import AnalysisException
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


# move this class to SPF
class FrameworkKafkaReader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        kafka_brokers: str,
        topic: str,
        schema: Any,
        starting_offset: int = -2,
        use_ssl: bool = True,
        previous_checkpoint_view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__()

        self.logger = get_logger(__name__)

        self.kafka_brokers: Param = Param(self, "kafka_brokers", "")
        self._setDefault(kafka_brokers=kafka_brokers)

        self.topic: Param = Param(self, "topic", "")
        self._setDefault(topic=topic)

        self.schema: Param = Param(self, "schema", "")
        self._setDefault(schema=schema)

        self.starting_offset: Param = Param(self, "starting_offset", "")
        self._setDefault(starting_offset=starting_offset)

        self.use_ssl: Param = Param(self, "use_ssl", "")
        self._setDefault(use_ssl=use_ssl)

        self.previous_checkpoint_view: Param = Param(
            self, "previous_checkpoint_view", ""
        )
        self._setDefault(previous_checkpoint_view=previous_checkpoint_view)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal
    @keyword_only
    def setParams(
        self,
        kafka_brokers: str,
        topic: str,
        schema: Any,
        starting_offset: int = -2,
        use_ssl: bool = True,
        previous_checkpoint_view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> Any:
        kwargs = self._input_kwargs
        super().setParams(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        kafka_brokers: str = self.getKafkaBrokers()
        topic: str = self.getTopic()
        starting_offset: int = self.getStartingOffset()
        use_ssl: bool = self.getUseSsl()
        schema: Any = self.getSchema()
        name: Optional[str] = self.getName()
        previous_checkpoint_view: Optional[str] = self.getPreviousCheckpointView()

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(
            name=f"{name or topic}_kafka_reader", progress_logger=progress_logger
        ):
            try:
                if previous_checkpoint_view in df.sql_ctx.tableNames():
                    last_offset = (
                        df.sql_ctx.table(previous_checkpoint_view)
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
                    df.sql_ctx.read.format("kafka")
                    .option("kafka.bootstrap.servers", kafka_brokers)
                    .option("kafka.security.protocol", security_protocol)
                    .option("subscribe", f"{topic}")
                    .option("startingOffsets", starting_offset_text)
                    .load()
                )
                df = df.selectExpr("*", "CAST(value AS STRING) as event")
                df = df.withColumn("event", from_json(df.event, schema))
                df.createOrReplaceTempView(topic)
                if len(df.head(1)) == 0:
                    df.sql_ctx.table(previous_checkpoint_view).createOrReplaceTempView(
                        f"{topic}_watermark"
                    )
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
        return self.getOrDefault(self.kafka_brokers)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getTopic(self) -> str:
        return self.getOrDefault(self.topic)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getStartingOffset(self) -> int:
        return self.getOrDefault(self.starting_offset)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getUseSsl(self) -> bool:
        return self.getOrDefault(self.use_ssl)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> int:
        return self.getOrDefault(self.schema)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getPreviousCheckpointView(self) -> Optional[str]:
        return self.getOrDefault(self.previous_checkpoint_view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getName(self) -> Optional[str]:
        return cast(str, self.getOrDefault(self.name)) or cast(
            str, self.getOrDefault(self.topic)
        )
