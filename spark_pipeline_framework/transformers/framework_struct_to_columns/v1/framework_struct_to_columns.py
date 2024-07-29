from typing import Dict, Any, Optional

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkStructToColumns(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        output_view: str,
        column_to_explode: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.view: Param = Param(self, "view", "")  # type: ignore
        self._setDefault(view=view)

        self.output_view: Param = Param(self, "output_view", "")  # type: ignore
        self._setDefault(output_view=output_view)

        self.column_to_explode: Param = Param(self, "column_to_explode", "")  # type: ignore
        self._setDefault(column_to_explode=column_to_explode)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        output_view: str = self.getOutputView()
        column_to_explode: str = self.getColumnToExplode()

        source_df = df.sparkSession.table(view)
        output_df = source_df.select(
            explode(column_to_explode).alias(column_to_explode)
        )
        output_df.createOrReplaceTempView(output_view)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getOutputView(self) -> str:
        return self.getOrDefault(self.output_view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumnToExplode(self) -> str:
        return self.getOrDefault(self.column_to_explode)  # type: ignore
