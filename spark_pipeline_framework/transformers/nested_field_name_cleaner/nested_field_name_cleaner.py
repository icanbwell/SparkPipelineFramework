from typing import Dict, Any, Optional

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.pyspark_helpers.clean_column_name import (
    replace_characters_in_nested_fields,
)


class FrameworkNestedFieldNameCleaner(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        column_name: str,
        name: Optional[str] = None,
        view: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        This transformer replaces the spaces in field names in the `column_name` columns with _


        :param column_name: column in which to replace field names
        :param name: name of transformer for logging
        :param view: (Optional) view to read data from and to update data in
        :param parameters: parameters
        :param progress_logger:
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        assert column_name

        self.column_name: Param[str] = Param(self, "column_name", "")
        self._setDefault(column_name=column_name)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=view)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        column_name: str = self.getColumnName()
        # name: Optional[str] = self.getName()

        if view:
            df = df.sparkSession.table(view)

        df = replace_characters_in_nested_fields(df=df, column_name=column_name)

        if view:
            df.createOrReplaceTempView(view)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumnName(self) -> str:
        return self.getOrDefault(self.column_name)
