from typing import Dict, Any, Optional, List

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkDropDuplicatesTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        view: str,
        columns: List[str],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        assert view
        assert columns
        assert isinstance(columns, list)

        # add a param
        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.columns: Param[List[str]] = Param(self, "columns", "")
        self._setDefault(columns=columns)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        columns: List[str] = self.getColumns()

        result_df: DataFrame = df.sql_ctx.table(view)
        result_df = result_df.drop_duplicates(columns)
        result_df.createOrReplaceTempView(view)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumns(self) -> List[str]:
        return self.getOrDefault(self.columns)
