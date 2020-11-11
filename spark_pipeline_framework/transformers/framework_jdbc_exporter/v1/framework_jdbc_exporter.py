from typing import Any, Dict

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.transformers.framework_base_exporter.v1.framework_base_exporter import \
    FrameworkBaseExporter


class FrameworkJdbcExporter(FrameworkBaseExporter):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self, jdbc_url: str, table: str, driver: str, **kwargs: Dict[Any, Any]
    ):
        super().__init__(**kwargs)
        assert jdbc_url
        assert table
        assert driver

        self.logger = get_logger(__name__)

        self.jdbc_url: Param = Param(self, "jdbc_url", "")
        self._setDefault(jdbc_url=jdbc_url)

        self.table: Param = Param(self, "table", "")
        self._setDefault(table=table)

        self.driver: Param = Param(self, "driver", "")
        self._setDefault(driver=driver)

        self._set(jdbc_url=jdbc_url, table=table, driver=driver)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setJdbcUrl(self, value: Param) -> 'FrameworkJdbcExporter':
        self._paramMap[self.jdbc_url] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getJdbcUrl(self) -> str:
        return self.getOrDefault(self.jdbc_url)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setTable(self, value: Param) -> 'FrameworkJdbcExporter':
        self._paramMap[self.table] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getTable(self) -> str:
        return self.getOrDefault(self.table)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setDriver(self, value: Param) -> 'FrameworkJdbcExporter':
        self._paramMap[self.driver] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDriver(self) -> str:
        return self.getOrDefault(self.driver)  # type: ignore

    def getFormat(self) -> str:
        return "jdbc"

    def getOptions(self) -> Dict[str, Any]:
        return {
            "url": self.getJdbcUrl(),
            "dbtable": self.getTable(),
            "driver": self.getDriver(),
        }
