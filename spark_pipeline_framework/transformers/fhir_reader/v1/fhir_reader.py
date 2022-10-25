from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Callable

from pyspark.sql.functions import col

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, DataType
from pyspark.sql.utils import AnalysisException
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_log_metric import (
    ProgressLogMetric,
)
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FhirReader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        file_path: Union[Path, str, Callable[[Optional[str]], Union[Path, str]]],
        view: Optional[str] = None,
        name: Optional[str] = None,
        schema: Optional[Union[StructType, DataType]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        bad_records_path: Optional[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = None,
        create_empty_view_if_file_path_not_found: Optional[bool] = False,
        limit: int = -1,
        encoding: Optional[str] = None,
        multi_line: bool = False,
        autodiscover_multiline: bool = False,
        filter_by_resource_type: Optional[str] = None,
    ):
        """
        Reads json files from path and creates columns corresponding to the FHIR schema


        :param file_path: path to the fhir data to read
        :param view: the view to load the data into
        :param name: a name to use when logging information about this transformer
        :param schema: the schema for the fhir data
        :param parameters: the parameter dictionary
        :param progress_logger: the progress logger
        :param bad_records_path: the path to write files that spark cannot parse
        :param create_empty_view_if_file_path_not_found: create an empty view using the provided schema in
                cases where the file_path may not exist.
                *NOTE if this is true and schema is not provided then the df will have no columns!
        :param multi_line: True = file is multi-line, False = file is ndjson
        :param autodiscover_multiline: Whether to read the first line of the file to discover whether it is multiline
        :param filter_by_resource_type: (Optional) read only these resource types from file
        """

        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert (
            isinstance(file_path, Path)
            or isinstance(file_path, str)
            or callable(file_path)
        ), type(file_path)
        assert file_path

        self.logger = get_logger(__name__)

        self.view: Param[Optional[str]] = Param(self, "view", "")
        self._setDefault(view=view)

        self.file_path: Param[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = Param(self, "file_path", "")
        self._setDefault(file_path=file_path)

        self.limit: Param[int] = Param(self, "limit", "")
        self._setDefault(limit=None)

        self.schema: Param[Optional[Union[StructType, DataType]]] = Param(
            self, "schema", ""
        )
        self._setDefault(schema=None)

        self.bad_records_path: Param[
            Optional[Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]]
        ] = Param(self, "bad_records_path", "")
        self._setDefault(bad_records_path=None)

        self.create_empty_view_if_file_path_not_found: Param[Optional[bool]] = Param(
            self, "create_empty_view_if_file_path_not_found", ""
        )
        self._setDefault(
            create_empty_view_if_file_path_not_found=create_empty_view_if_file_path_not_found
        )

        self.encoding: Param[Optional[str]] = Param(self, "encoding", "")
        self._setDefault(encoding=encoding)

        self.multiLine: Param[bool] = Param(self, "multiLine", "")
        self._setDefault(multiLine=multi_line)
        self._set(multiLine=multi_line)

        self.autodiscover_multiline: Param[bool] = Param(
            self, "autodiscover_multiline", ""
        )
        self._setDefault(autodiscover_multiline=autodiscover_multiline)

        self.filter_by_resource_type: Param[Optional[str]] = Param(
            self, "filter_by_resource_type", ""
        )
        self._setDefault(filter_by_resource_type=None)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def preprocess(self, df: DataFrame, absolute_paths: List[str]) -> None:
        """
        In pre-processing we try to detect whether the file is a normal json or ndjson
        :param df: DataFrame
        :param absolute_paths: list of paths
        """

        if not self.getAutoDiscoverMultiLine():
            return

        assert absolute_paths
        text_df: DataFrame = df.sql_ctx.read.text(absolute_paths)
        # read the first line of the file
        first_line: str = text_df.select("value").limit(1).collect()[0][0]
        if (
            first_line is not None
            and first_line.lstrip().startswith("[")
            or first_line.lstrip().rstrip() == "{"
        ):
            self.setMultiLine(True)
        else:
            self.setMultiLine(False)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: Optional[str] = self.getView()
        file_path: Union[
            Path, str, Callable[[Optional[str]], Union[Path, str]]
        ] = self.getFilePath()
        if callable(file_path):
            file_path = file_path(self.loop_id)
        name: Optional[str] = self.getName()
        schema: Optional[Union[StructType, DataType]] = self.getSchema()
        bad_records_path: Optional[
            Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]
        ] = self.getBadRecordsPath()
        if callable(bad_records_path):
            bad_records_path = bad_records_path(self.loop_id)
        create_empty_view_if_file_path_not_found: Optional[
            bool
        ] = self.getCreateEmptyViewIfFilePathNotFound()
        encoding: Optional[str] = self.getEncoding()

        if schema is not None:
            assert isinstance(schema, StructType), type(schema)
        progress_logger: Optional[ProgressLogger] = self.getProgressLogger()

        filter_by_resource_type: Optional[str] = self.getFilterByResourceType()
        # limit: int = self.getLimit()

        with ProgressLogMetric(
            name=f"{name or view}_fhir_reader", progress_logger=progress_logger
        ):
            try:
                self.preprocess(df=df, absolute_paths=[str(file_path)])

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
                reader = df.sql_ctx.read
                if bad_records_path:
                    reader = reader.option("badRecordsPath", str(bad_records_path))
                if schema:
                    reader = reader.schema(schema)
                if encoding:
                    reader = reader.option("encoding", encoding)
                for k, v in self.getReaderOptions().items():
                    reader = reader.option(k, v)
                df = reader.json(str(file_path))

                assert (
                    "_corrupt_record" not in df.columns
                ), f"Found _corrupt_record after reading the file: {file_path}. "

                if "id" in df.columns:
                    df = df.dropDuplicates(["id"])

                if filter_by_resource_type:
                    df = df.where(
                        col("resourceType").eqNullSafe(filter_by_resource_type)
                    )

                if view:
                    df.createOrReplaceTempView(view)

                self.logger.info(
                    f"Read {df.count()} records from json file {file_path}"
                )

            except AnalysisException as e:
                self.logger.exception(f"File read failed from {file_path}")
                if create_empty_view_if_file_path_not_found and view:
                    if schema:
                        df = df.sql_ctx.createDataFrame(
                            df.sql_ctx.sparkSession.sparkContext.emptyRDD(), schema
                        )
                        df.createOrReplaceTempView(view)
                    else:
                        df = df.sql_ctx.createDataFrame(
                            df.sql_ctx.sparkSession.sparkContext.emptyRDD()
                        )
                        df.createOrReplaceTempView(view)
                else:
                    raise e
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> Optional[str]:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilePath(
        self,
    ) -> Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]:
        return self.getOrDefault(self.file_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getLimit(self) -> int:
        return self.getOrDefault(self.limit)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> Optional[Union[StructType, DataType]]:
        return self.getOrDefault(self.schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getBadRecordsPath(
        self,
    ) -> Optional[Union[Path, str, Callable[[Optional[str]], Union[Path, str]]]]:
        return self.getOrDefault(self.bad_records_path)

    # noinspection PyPep8Naming
    def getCreateEmptyViewIfFilePathNotFound(self) -> Optional[bool]:
        return self.getOrDefault(self.create_empty_view_if_file_path_not_found)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getEncoding(self) -> Optional[str]:
        return self.getOrDefault(self.encoding)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMultiLine(self) -> bool:
        return self.getOrDefault(self.multiLine)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAutoDiscoverMultiLine(self) -> bool:
        return self.getOrDefault(self.autodiscover_multiline)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def setMultiLine(self, value: bool) -> "FhirReader":
        # noinspection PyUnresolvedReferences
        self._paramMap[self.multiLine] = value
        return self

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getReaderOptions(self) -> Dict[str, Any]:
        options: Dict[str, Any] = {"multiLine": self.getMultiLine()}
        return options

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFilterByResourceType(self) -> Optional[str]:
        return self.getOrDefault(self.filter_by_resource_type)

    def as_dict(self) -> Dict[str, Any]:
        my_dict: Dict[str, Any] = super().as_dict()
        my_dict["params"] = {
            k: v for k, v in my_dict["params"].items() if k != "schema"
        }  # schema is just too huge
        return my_dict
