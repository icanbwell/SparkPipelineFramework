import os

from smart_open import open as smart_open  # type: ignore
from typing import Optional, Dict, Any, List

from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.logger.yarn_logger import get_logger

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.file_helpers import isfile, listdir

pipeline_validation_df_name = "pipeline_validation"


class FrameworkValidationTransformer(FrameworkTransformer):
    """
    The FrameworkValidationTransformer will run all SQL based validations in the paths specified and optionally fail the
    pipeline if validations fail. The results of validation will be stored in a dataframe and persisted with a temp table
    named 'pipeline_validation'
    """

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        validation_source_path: str,
        validation_queries: List[str],
        fail_on_validation: bool = False,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """

        :param validation_source_path: the root path to the validation queries
        :param validation_queries: a list of validation queries paths relative to the root path, can be the path of a file relative to the root path, or a path to a directory relative to root path
        :param fail_on_validation: if True, fails the pipeline at this transformer, defaults to False
        :param name: a name for the transformer step
        :param parameters: parameters
        :param progress_logger: the logger to use for logging
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        self.logger = get_logger(__name__)

        if not validation_source_path:
            raise ValueError("validation source path is None or empty")

        if not validation_queries:
            raise ValueError("validation_queries is None")

        self.validation_source_path: Param[str] = Param(
            self, "validation_source_path", ""
        )
        self._setDefault(validation_source_path=None)

        self.validation_queries: Param[List[str]] = Param(
            self, "validation_queries", ""
        )
        self._setDefault(validation_queries=None)

        self.fail_on_validation: Param[bool] = Param(self, "fail_on_validation", "")
        self._setDefault(fail_on_validation=False)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        validation_source_path = self.getValidationSourcePath()
        validation_queries = self.getValidationQueries()
        fail_on_validation = self.getFailOnValidation()

        for query in validation_queries:
            path = os.path.join(validation_source_path, query)
            self._validate(path, df)

        if fail_on_validation:
            errors_df = df.sql_ctx.sql(
                f"SELECT * from {pipeline_validation_df_name} where is_failed == 1"
            )
            error_count = errors_df.count()
            assert (
                error_count == 0
            ), f"Pipeline failed validation, there were {error_count} errors. See additional logs or details"
        return df

    def _validate(
        self,
        path: str,
        df: DataFrame,
    ) -> None:
        validation_df = self.get_validation_df(df)
        if isfile(path):
            self.logger.info(f"Path: {path} is a file")
            with smart_open(path, "r") as query_file:
                self.logger.info(f"Executing validation query: {path}")
                query_text = query_file.read()
                query_text = query_text.upper().replace(
                    "SELECT", f"SELECT '{path}' as query,\n"
                )
                if validation_df:
                    validation_df = validation_df.union(df.sql_ctx.sql(query_text))
                    validation_df.createOrReplaceTempView(pipeline_validation_df_name)
                else:
                    validation_df = df.sql_ctx.sql(query_text)
                    validation_df.createOrReplaceTempView(pipeline_validation_df_name)
        else:
            self.logger.info(f"Path: {path} is a directory, getting paths")
            paths = listdir(path)
            self.logger.info(f"Got paths: {paths}")
            for child_path in paths:
                self._validate(child_path, df)

    def get_validation_df(self, df: DataFrame) -> Optional[DataFrame]:
        validation_df: Optional[DataFrame] = None
        tables_df = df.sql_ctx.tables().filter(
            f"tableName ='{pipeline_validation_df_name}'"
        )
        if tables_df.count() == 1:
            validation_df = df.sql_ctx.table(pipeline_validation_df_name)
        return validation_df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getValidationSourcePath(self) -> str:
        return self.getOrDefault(self.validation_source_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getValidationQueries(self) -> List[str]:
        return self.getOrDefault(self.validation_queries)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFailOnValidation(self) -> bool:
        return self.getOrDefault(self.fail_on_validation)
