from typing import (
    Protocol,
    List,
    Dict,
    Any,
    Optional,
    TypeVar,
    AsyncGenerator,
    Union,
    TypeAlias,
)

# By declaring T as contravariant=True, you indicate that T can be a more general type than the actual type used,
# which aligns with how Protocol expects type variables to be used in this context.
T = TypeVar("T", contravariant=True)
TInputColumnDataType = TypeVar(
    "TInputColumnDataType", Dict[str, Any], Union[int, float, str, bool]
)
TOutputColumnDataType = TypeVar(
    "TOutputColumnDataType",
    covariant=True,
    bound=Dict[str, Any] | Union[int, float, str, bool],
)


class HandlePandasBatchFunction(
    Protocol[T, TInputColumnDataType, TOutputColumnDataType]
):
    """
    This is the definition of the function that is called by the Pandas UDF. This function is called with a batch of
    input values and should return a batch of output values.

    T is the type of the parameters passed to the Pandas UDF.
    TDataType is the type of input data.  It can be dict for struct columns otherwise the type of
    the column
    """

    async def __call__(
        self,
        *,
        partition_index: int,
        chunk_index: int,
        chunk_input_range: range,
        input_values: List[TInputColumnDataType],
        parameters: Optional[T],
    ) -> AsyncGenerator[TOutputColumnDataType, None]:
        """
        This function is called with a batch of input values and should return a batch of output values.

        :param partition_index: The index of the partition.
        :param chunk_index: The index of the chunk within the partition.
        :param chunk_input_range: The range of indices of the input values within the chunk.
        :param input_values: input values as a list of dictionaries
        :param parameters: additional parameters passed to the Pandas UDF
        :return: output values as a list of dictionaries
        """
        ...


"""
This is the type alias for when we are operating on a full dataframe not on specific columns
"""
HandlePandasDataFrameBatchFunction: TypeAlias = HandlePandasBatchFunction[
    T, Dict[str, Any], Dict[str, Any]
]

"""
This is the type alias for when we are operating on a single column for type struct, and we want to output a struct
"""
HandlePandasStructToStructBatchFunction: TypeAlias = HandlePandasBatchFunction[
    T, Dict[str, Any], Dict[str, Any]
]

"""
This is the type alias for when we are operating on a single column for type scalar, and we want to output a struct
"""
HandlePandasScalarToStructBatchFunction: TypeAlias = HandlePandasBatchFunction[
    T, Union[int, float, str, bool], Dict[str, Any]
]


"""
This is the type alias for when we are operating on a single column for type scalar, and we want to output a struct
"""
HandlePandasScalarToScalarBatchFunction: TypeAlias = HandlePandasBatchFunction[
    T, Union[int, float, str, bool], Union[int, float, str, bool]
]

"""
This is the type alias for when we are operating on a single column for type struct, and we want to output a scalar
"""
HandlePandasStructToScalarBatchFunction: TypeAlias = HandlePandasBatchFunction[
    T, Dict[str, Any], Union[int, float, str, bool]
]
