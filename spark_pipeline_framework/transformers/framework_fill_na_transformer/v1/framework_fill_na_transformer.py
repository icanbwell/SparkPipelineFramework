from typing import List, Optional, Any, Dict

from pyspark import keyword_only
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
    Replace NA/Null values with a specified replacement_value for specified columns_to_check for null values
    """

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        replacement_value: Any,
        columns_to_check: List[str],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__()
        self.logger = get_logger(__name__)

        self.view: Param = Param(self, "view", "")
        self._setDefault(view=view)

        self.replacement_value: Param = Param(self, "replacement_value", "")
        self._setDefault(replacement_value=replacement_value)

        self.columns_to_check: Param = Param(self, "columns_to_check", "")
        self._setDefault(columns_to_check=columns_to_check)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
        self,
        view: str,
        replacement_value: Any,
        columns_to_check: List[str],
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
        columns_to_fill: List[str] = self.getColumnsToCheck()
        replacement_value: Any = self.getReplacementValue()
        view: str = self.getView()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        with ProgressLogMetric(name=f"{view}_fill_na", progress_logger=progress_logger):
            self.logger.info(
                f"filling rows if any null values with replacement_value found for columns: {columns_to_fill}"
            )
            df_with_na: DataFrame = df.sql_ctx.table(view)
            data_types = get_dtype(df_with_na, columns_to_fill)

            if type(replacement_value) in (list, dict):
                assert len(replacement_value) == len(columns_to_fill), f"If replacement_value is a list or dictionary, the values must be equal to the number of columns in columns to fill. {len(replacement_value)} != {len(columns_to_fill)}"

            for col_idx, col in enumerate(columns_to_fill):

                if type(replacement_value) == dict:
                    value = replacement_value.get(col, None)
                elif type(replacement_value) == list:
                    value = replacement_value[col_idx]
                else:
                    value = replacement_value

                if data_types[col] != "string":
                    try:
                        value = float(value)
                    except Exception as e:
                        print(str(e))
                        print(f"The data type of column: {col} is {data_types[col]}. Either cast the column as a StringType or change the type of the value you are feeding as the replacement value to a string type.")

                df_with_filled_na = df_with_na.na.fill(
                    value=value, subset=col
                )

                df_with_filled_na.createOrReplaceTempView(view)
        return df_with_filled_na

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumnsToCheck(self) -> List[str]:
        return self.getOrDefault(self.columns_to_check)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReplacementValue(self) -> Any:
        return self.getOrDefault(self.replacement_value)
