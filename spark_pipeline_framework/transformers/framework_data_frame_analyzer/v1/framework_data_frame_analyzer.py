import os
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkDataFrameAnalyzer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        analysis_views_prefix: Optional[str] = None,
        output_folder: Optional[Union[Path, str]] = None,
        columns_to_analyze: Optional[List[str]] = None,
        columns_to_skip: Optional[List[str]] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """
        For each column in the view, this transformer can either:
        1. Create a view that contains the unique values and the count of each
        2. Write a csv to output_folder that contains the unique values and the count of each
        :param view: view to load the data from
        :param analysis_views_prefix: (Optional) prefix to use when creating the analysis views
        :param output_folder: (Optional) folder in which to create the csvs
        :param columns_to_analyze: (Optional) limit analysis to these columns
        :param columns_to_skip: (Optional) don't include these columns in analysis
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        assert view
        assert output_folder

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.analysis_views_prefix: Param[Optional[str]] = Param(
            self, "analysis_views_prefix", ""
        )
        self._setDefault(analysis_views_prefix=analysis_views_prefix)

        self.output_folder: Param[Optional[Union[Path, str]]] = Param(
            self, "output_folder", ""
        )
        self._setDefault(output_folder=output_folder)

        self.columns_to_analyze: Param[Optional[List[str]]] = Param(
            self, "columns_to_analyze", ""
        )
        self._setDefault(columns_to_analyze=columns_to_analyze)

        self.columns_to_skip: Param[Optional[List[str]]] = Param(
            self, "columns_to_skip", ""
        )
        self._setDefault(columns_to_skip=columns_to_skip)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        analysis_views_prefix: Optional[str] = self.getAnalysisViewsPrefix()
        output_folder: Optional[Union[Path, str]] = self.getOutputFolder()
        columns_to_analyze: Optional[List[str]] = self.getColumnsToAnalyze()
        columns_to_skip: Optional[List[str]] = self.getColumnsToSkip()
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        # get columns in data frame
        df = df.sql_ctx.table(view)

        if columns_to_analyze:
            columns_to_analyze = [c for c in df.columns if c in columns_to_analyze]
        else:
            columns_to_analyze = df.columns

        assert columns_to_analyze

        if columns_to_skip:
            columns_to_analyze = [
                c for c in columns_to_analyze if c not in columns_to_skip
            ]

        column_name: str
        for column_name in columns_to_analyze:
            result_df: DataFrame = (
                df.select(column_name)
                .groupBy(column_name)
                .count()
                .orderBy(col("count").desc())
            )
            if output_folder:
                target_path: str = str(os.path.join(str(output_folder), column_name))
                if progress_logger:
                    progress_logger.write_to_log(
                        f"Writing analysis for column {column_name} to {target_path}"
                    )
                result_df.coalesce(1).write.csv(
                    target_path, header=True, mode="overwrite"
                )
            if analysis_views_prefix:
                result_df.createOrReplaceTempView(
                    f"{analysis_views_prefix}{column_name}"
                    if analysis_views_prefix.endswith("_")
                    else f"{analysis_views_prefix}_{column_name}"
                )

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAnalysisViewsPrefix(self) -> Optional[str]:
        return self.getOrDefault(self.analysis_views_prefix)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getOutputFolder(self) -> Optional[Union[Path, str]]:
        return self.getOrDefault(self.output_folder)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumnsToAnalyze(self) -> Optional[List[str]]:
        return self.getOrDefault(self.columns_to_analyze)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getColumnsToSkip(self) -> Optional[List[str]]:
        return self.getOrDefault(self.columns_to_skip)
