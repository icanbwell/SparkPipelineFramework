from typing import Dict, Any, Optional, List

from pyspark.ml.param import Param
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)

# noinspection PyPep8Naming
from pyspark.sql import functions as F
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_table_exists,
)


class FrameworkGroupByTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        group_by_columns: List[str],
        source_view: str,
        view: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        append_to_view: Optional[bool] = None,
        concatenate_content_as_json: Optional[bool] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert source_view
        assert view
        assert group_by_columns is not None
        assert len(group_by_columns) > 0

        self.logger = get_logger(__name__)

        self.group_by_column: Param[str] = Param(self, "group_by_column", "")
        self._setDefault(group_by_column=group_by_columns)

        self.source_view: Param[str] = Param(self, "source_view", "")
        self._setDefault(source_view=source_view)

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.append_to_view: Param[Optional[bool]] = Param(self, "append_to_view", "")
        self._setDefault(append_to_view=append_to_view)

        self.concatenate_content_as_json: Param[Optional[bool]] = Param(
            self, "concatenate_content_as_json", ""
        )
        self._setDefault(concatenate_content_as_json=concatenate_content_as_json)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        source_view: str = self.getOrDefault(self.source_view)
        view: str = self.getOrDefault(self.view)
        append_to_view: Optional[bool] = self.getOrDefault(self.append_to_view)
        concatenate_content_as_json: Optional[bool] = self.getOrDefault(
            self.concatenate_content_as_json
        )
        group_by_column: str = self.getOrDefault(self.group_by_column)

        df_grouped: DataFrame
        if concatenate_content_as_json:
            df_grouped = df.groupBy(*group_by_column).agg(
                F.count("*").alias("count"),
                F.concat_ws(
                    ",",
                    F.collect_list(F.to_json(F.struct([df[x] for x in df.columns]))),
                ).alias("json"),
            )
        else:
            df_grouped = df.groupBy(group_by_column).agg(F.count("*").alias("count"))

        if append_to_view:
            if spark_table_exists(df.sql_ctx, view):
                df_grouped = df.sparkSession.table(view).union(df_grouped)

        df_grouped.createOrReplaceTempView(view)

        return df
