from pathlib import Path
from typing import Dict, Any, Optional, Union

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FhirTextReader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        file_path: Union[Path, str],
        destination_column_name: str = "value",
        view: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        bad_records_path: Optional[Union[Path, str]] = None,
        create_empty_view_if_file_path_not_found: Optional[bool] = False,
        limit: int = -1,
    ):
        """
        Reads files from path and creates one column called destination_column_name that contains the FHIR data as text


        :param file_path: path to the fhir data to read
        :param view: the view to load the data into
        :param name: a name to use when logging information about this transformer
        :param destination_column_name: column to create
        :param parameters: the parameter dictionary
        :param progress_logger: the progress logger
        :param bad_records_path: the path to write files that spark cannot parse
        :param create_empty_view_if_file_path_not_found: create an empty view using the provided schema in cases where the
        file_path may not exist. *NOTE if this is true and schema is not provided then the df will have no columns!
        """

        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert isinstance(file_path, Path) or isinstance(file_path, str)

        assert file_path

        self.logger = get_logger(__name__)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=view)

        self.file_path: Param[Union[Path, str]] = Param(self, "file_path", "")
        self._setDefault(file_path=None)

        self.destination_column_name: Param[str] = Param(
            self, "destination_column_name", ""
        )
        self._setDefault(destination_column_name=destination_column_name)

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.bad_records_path: Param[Optional[Union[Path, str]]] = Param(
            self, "bad_records_path", ""
        )
        self._setDefault(bad_records_path=None)

        self.create_empty_view_if_file_path_not_found: Param[Optional[bool]] = Param(
            self, "create_empty_view_if_file_path_not_found", ""
        )
        self._setDefault(
            create_empty_view_if_file_path_not_found=create_empty_view_if_file_path_not_found
        )

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        path: Union[Path, str] = self.getFilePath()
        name: Optional[str] = self.getName()
        destination_column_name: str = self.getDestinationColumnName()
        bad_records_path: Optional[Union[Path, str]] = self.getBadRecordsPath()
        create_empty_view_if_file_path_not_found: Optional[bool] = (
            self.getCreateEmptyViewIfFilePathNotFound()
        )

        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
        # limit: int = self.getLimit()

        with ProgressLogMetric(
            name=f"{name or view}_fhir_text_reader", progress_logger=progress_logger
        ):
            try:
                # drop malformed records:
                # https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/DataFrameReader.html#json-scala.collection.Seq-
                # mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing.
                # PERMISSIVE : when it meets a corrupted record, puts the malformed string into a field configured
                #   by columnNameOfCorruptRecord, and sets other fields to null. To keep corrupt records,
                #   an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema.
                #   If a schema does not have the field, it drops corrupt records during parsing.
                #   When inferring a schema, it implicitly adds a columnNameOfCorruptRecord field in an
                #   output schema.
                # DROPMALFORMED : ignores the whole corrupted records.
                # FAILFAST : throws an exception when it meets corrupted records.
                # https://docs.databricks.com/spark/latest/spark-sql/handling-bad-records.html
                reader = df.sparkSession.read
                if bad_records_path:
                    reader = reader.option("badRecordsPath", str(bad_records_path))
                df = reader.text(str(path))

                if destination_column_name != "value":
                    df = df.withColumnRenamed("value", destination_column_name)

                if view:
                    df.createOrReplaceTempView(view)

                self.logger.info(f"Read {df.count()} records from text file {path}")

            except AnalysisException as e:
                self.logger.exception(f"File read failed from {path}")
                if create_empty_view_if_file_path_not_found and view:
                    df.createOrReplaceTempView(view)
                else:
                    raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getDestinationColumnName(self) -> str:
        return self.getOrDefault(self.destination_column_name)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(self) -> Union[Path, str]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getBadRecordsPath(self) -> Optional[Union[Path, str]]:
        return self.getOrDefault(self.bad_records_path)

    # noinspection PyPep8Naming
    def getCreateEmptyViewIfFilePathNotFound(self) -> Optional[bool]:
        return self.getOrDefault(self.create_empty_view_if_file_path_not_found)
