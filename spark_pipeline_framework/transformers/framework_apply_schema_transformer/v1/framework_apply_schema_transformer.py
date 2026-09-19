from typing import Dict, Any, Optional

# noinspection PyProtectedMember
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, DataType
from pyspark.sql.functions import lit
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class FrameworkApplySchemaTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        view: str,
        schema: StructType,
    ) -> None:
        """
        Takes a schema and applies it to an existing view

        :param name: a name for the transformer step
        :param parameters: a dictionary of parameters
        :param progress_logger: the logger object to be used for logging
        :param view: the view that shall have the schema applied
        :param schema: a schema StructType object that shall be applied
        :return: None (modifies the view in place)
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param
        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=view)

        self.schema: Param[StructType] = Param(self, "schema", "")
        self._setDefault(schema=schema)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        df = df.sparkSession.table(view)
        schema: StructType = self.getSchema()
        field: StructField
        for field in schema.fields:
            column_name: str = field.name
            column_type: DataType = field.dataType
            if column_name not in df.columns:
                df = df.withColumn(column_name, lit(None).cast(column_type))
        df.createOrReplaceTempView(view)
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSchema(self) -> StructType:
        return self.getOrDefault(self.schema)
