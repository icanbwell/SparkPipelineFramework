from typing import Optional, List, Dict, Any

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer import FrameworkTransformer


class FrameworkSelectColumnsTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(self,
                 view: str = None,
                 drop_columns: Optional[List[str]] = None,
                 keep_columns: Optional[List[str]] = None,
                 name: str = None,
                 parameters: Dict[str, Any] = None,
                 progress_logger: ProgressLogger = None,
                 verify_count_remains_same: bool = False
                 ) -> None:
        super().__init__(name=name, parameters=parameters, progress_logger=progress_logger)
        self.logger = get_logger(__name__)

        if not view:
            raise ValueError("view is None or empty")

        self.view: Param = Param(self, "view", "")
        self._setDefault(view=None)  # type: ignore

        self.drop_columns: Param = Param(self, "drop_columns", "")
        self._setDefault(drop_columns=None)  # type: ignore

        self.keep_columns: Param = Param(self, "keep_columns", "")
        self._setDefault(keep_columns=None)  # type: ignore

        self.verify_count_remains_same: Param = Param(self, "verify_count_remains_same", "")
        self._setDefault(verify_count_remains_same=None)  # type: ignore

        kwargs = self._input_kwargs  # type: ignore
        self.setParams(**kwargs)

    # noinspection PyUnusedLocal,PyMissingOrEmptyDocstring,PyPep8Naming
    @keyword_only
    def setParams(self,
                  view: str = None,
                  drop_columns: Optional[List[str]] = None,
                  keep_columns: Optional[List[str]] = None,
                  name: str = None,
                  parameters: Dict[str, Any] = None,
                  progress_logger: ProgressLogger = None,
                  verify_count_remains_same: bool = False
                  ):
        kwargs = self._input_kwargs  # type: ignore
        super().setParams(name=name, parameters=parameters, progress_logger=progress_logger)
        return self._set(**kwargs)  # type: ignore

    def _transform(self, df: DataFrame) -> DataFrame:
        # name = self.getName()
        view = self.getView()
        # progress_logger: ProgressLogger = self.getProgressLogger()
        drop_columns: Optional[List[str]] = self.getOrDefault("drop_columns")
        keep_columns: Optional[List[str]] = self.getOrDefault("keep_columns")

        result_df: DataFrame = df.sql_ctx.table(view)

        if keep_columns and len(keep_columns) > 0:
            all_columns = result_df.columns
            drop_columns = list(set(all_columns).difference(set(keep_columns)))

        if drop_columns and len(drop_columns) > 0:
            self.logger.info(f"FrameworkSelectColumnsTransformer: Dropping columns {drop_columns} from {view}")
            result_df.drop(*drop_columns).createOrReplaceTempView(view)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setSql(self, value):
        self._paramMap[self.sql] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSql(self) -> str:
        return self.getOrDefault(self.sql)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setView(self, value):
        self._paramMap[self.view] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setLogSql(self, value):
        self._paramMap[self.log_sql] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLogSql(self) -> str:
        return self.getOrDefault(self.log_sql)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setVerifyCountRemainsSame(self, value):
        self._paramMap[self.verify_count_remains_same] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getVerifyCountRemainsSame(self) -> bool:
        return self.getOrDefault(self.verify_count_remains_same)  # type: ignore
