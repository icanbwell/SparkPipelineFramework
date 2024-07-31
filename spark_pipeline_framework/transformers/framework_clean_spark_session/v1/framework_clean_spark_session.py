from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    create_empty_dataframe,
)


class FrameworkCleanSparkSession(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        Cleans out all the temp tables in memory


        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        # self.view: Param[str] = Param(self, "view", "")
        # self._setDefault(view=view)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        FrameworkCleanSparkSession.clean_spark_session(df.sparkSession)
        df = create_empty_dataframe(df.sparkSession)
        return df

    @staticmethod
    def clean_spark_session(session: SparkSession) -> None:
        """

        :param session:
        :return:
        """
        tables = session.catalog.listTables("default")

        for table in tables:
            print(f"Dropping table: {table.name}")
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            session.sql(f"DROP TABLE IF EXISTS default.{table.name}")
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            session.sql(f"DROP VIEW IF EXISTS default.{table.name}")
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            session.sql(f"DROP VIEW IF EXISTS {table.name}")

        session.catalog.clearCache()
