from typing import Union

from helixcore.utilities.data_frame_types.data_frame_types import (
    DataFrameType,
    DataFrameStringType,
    DataFrameIntegerType,
    DataFrameTimestampType,
    DataFrameBooleanType,
    DataFrameFloatType,
    DataFrameStructType,
    DataFrameArrayType,
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    TimestampType,
    BooleanType,
    FloatType,
    StructType,
    ArrayType,
    StructField,
)


class SparkTypeConverter:
    @staticmethod
    def convert_type(
        df_type: DataFrameType | DataFrameStructType,
    ) -> Union[
        StringType,
        IntegerType,
        TimestampType,
        BooleanType,
        FloatType,
        StructType,
        ArrayType,
    ]:
        """
        Convert DataFrameType to corresponding Spark type

        Args:
            df_type (DataFrameType): Custom DataFrame type to convert

        Returns:
            Corresponding Spark type
        """
        if isinstance(df_type, DataFrameStringType):
            return StringType()
        elif isinstance(df_type, DataFrameIntegerType):
            return IntegerType()
        elif isinstance(df_type, DataFrameTimestampType):
            return TimestampType()
        elif isinstance(df_type, DataFrameBooleanType):
            return BooleanType()
        elif isinstance(df_type, DataFrameFloatType):
            return FloatType()
        elif isinstance(df_type, DataFrameStructType):
            return SparkTypeConverter.convert_struct_type(df_type)
        elif isinstance(df_type, DataFrameArrayType):
            return ArrayType(SparkTypeConverter.convert_type(df_type.item_type))
        else:
            raise ValueError(f"Unsupported type: {type(df_type)}")

    @staticmethod
    def convert_struct_type(struct_type: DataFrameStructType) -> StructType:
        """
        Recursively convert DataFrameStructType to Spark StructType

        Args:
            struct_type (DataFrameStructType): Custom struct type to convert

        Returns:
            Spark StructType
        """
        assert isinstance(struct_type, DataFrameStructType)

        spark_fields = []
        for field in struct_type.fields:
            spark_fields.append(
                StructField(
                    field.name,
                    SparkTypeConverter.convert_type(field.data_type),
                    field.nullable,
                )
            )
        return StructType(spark_fields)
