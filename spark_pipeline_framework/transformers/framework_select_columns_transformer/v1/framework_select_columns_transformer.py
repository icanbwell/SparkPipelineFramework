from typing import Optional, List, Dict, Any

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkSelectColumnsTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: Optional[str] = None,
        drop_columns: Optional[List[str]] = None,
        keep_columns: Optional[List[str]] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        verify_count_remains_same: bool = False,
    ) -> None:
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        self.logger = get_logger(__name__)

        if not view:
            raise ValueError("view is None or empty")

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=None)

        self.drop_columns: Param[Optional[List[str]]] = Param(self, "drop_columns", "")
        self._setDefault(drop_columns=None)

        self.keep_columns: Param[Optional[List[str]]] = Param(self, "keep_columns", "")
        self._setDefault(keep_columns=None)

        self.verify_count_remains_same: Param[bool] = Param(
            self, "verify_count_remains_same", ""
        )
        self._setDefault(verify_count_remains_same=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        # name = self.getName()
        view = self.getView()
        # progress_logger: ProgressLogger = self.getProgressLogger()
        drop_columns: Optional[List[str]] = self.getOrDefault(self.drop_columns)
        keep_columns: Optional[List[str]] = self.getOrDefault(self.keep_columns)

        assert view
        result_df: DataFrame = df.sparkSession.table(view)

        if keep_columns and len(keep_columns) > 0:
            all_columns = result_df.columns
            drop_columns = list(set(all_columns).difference(set(keep_columns)))

        if drop_columns and len(drop_columns) > 0:
            self.logger.info(
                f"FrameworkSelectColumnsTransformer: Dropping columns {drop_columns} from {view}"
            )
            result_df.drop(*drop_columns).createOrReplaceTempView(view)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getVerifyCountRemainsSame(self) -> bool:
        return self.getOrDefault(self.verify_count_remains_same)
