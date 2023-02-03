from typing import List, Optional, Any, Dict, Union

from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


def get_dtype(df: DataFrame, colnames: List[str]) -> Dict[str, str]:
    return dict(df.select(colnames).dtypes)


class FrameworkFillNaTransformer(FrameworkTransformer):
    """
    Replace NA/Null values with a specified value for each column in the dictionary
    """

    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        column_mapping: Dict[str, Any],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__()
        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.column_mapping: Param[Dict[str, Any]] = Param(self, "column_mapping", "")
        self._setDefault(column_mapping=column_mapping)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        column_mapping: Dict[str, Any] = self.getColumnMapping()
        view: str = self.getView()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(name=f"{view}_fill_na", progress_logger=progress_logger):
            self.logger.info(
                f"filling rows if any null values with replacement_value found for columns: {list(column_mapping.keys())}"
            )
            df_with_na: DataFrame = df.sql_ctx.table(view)
            df_with_filled_na = df_with_na
            data_types = get_dtype(df_with_na, list(column_mapping.keys()))

            value: Union[bool, int, float, str]
            for col, value in column_mapping.items():
                if data_types[col] != "string":
                    try:
                        value = float(value)
                    except Exception as e:
                        print(str(e))
                        print(
                            f"The data type of column: {col} is {data_types[col]}. Either cast the column as a StringType or change the type of the value you are feeding as the replacement value to a string type."
                        )

                df_with_filled_na = df_with_filled_na.na.fill(value=value, subset=[col])

            df_with_filled_na.createOrReplaceTempView(view)
        return df_with_filled_na

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumnMapping(self) -> Dict[str, Any]:
        return self.getOrDefault(self.column_mapping)
