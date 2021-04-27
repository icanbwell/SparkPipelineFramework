from typing import Any, Dict, Optional

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_base_exporter.v1.framework_base_exporter import (
    FrameworkBaseExporter,
)


class FrameworkJdbcExporter(FrameworkBaseExporter):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        jdbc_url: str,
        table: str,
        driver: str,
        view: Optional[str] = None,
        name: Optional[str] = None,
        mode: str = FrameworkBaseExporter.MODE_ERROR,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        limit: int = -1,
        options: Dict[str, Any] = {},
    ):
        super().__init__(
            view=view,
            name=name,
            mode=mode,
            parameters=parameters,
            progress_logger=progress_logger,
            limit=limit,
        )
        assert jdbc_url
        assert table
        assert driver

        self.logger = get_logger(__name__)

        self.options: Param[Dict[str, Any]] = Param(self, "options", "")
        self._setDefault(options=options)

        self.jdbc_url: Param[str] = Param(self, "jdbc_url", "")
        self._setDefault(jdbc_url=jdbc_url)

        self.table: Param[str] = Param(self, "table", "")
        self._setDefault(table=table)

        self.driver: Param[str] = Param(self, "driver", "")
        self._setDefault(driver=driver)

        self._set(jdbc_url=jdbc_url, table=table, driver=driver, options=options)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getJdbcUrl(self) -> str:
        return self.getOrDefault(self.jdbc_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getTable(self) -> str:
        return self.getOrDefault(self.table)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDriver(self) -> str:
        return self.getOrDefault(self.driver)

    def getFormat(self) -> str:
        return "jdbc"

    def getOptions(self) -> Dict[str, Any]:
        options: Dict[str, Any] = self.getOrDefault(self.options).copy()
        parameter_options: Dict[str, Any] = {
            "url": self.getJdbcUrl(),
            "dbtable": self.getTable(),
            "driver": self.getDriver(),
        }
        options.update(parameter_options)
        return options
