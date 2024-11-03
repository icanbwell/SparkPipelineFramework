from typing import (
    Protocol,
    List,
    Dict,
    Any,
    Optional,
    AsyncGenerator,
    Union,
)

from spark_pipeline_framework.utilities.async_pandas_udf.v1.async_pandas_batch_function_run_context import (
    AsyncPandasBatchFunctionRunContext,
)

AcceptedParametersType = Dict[str, Any] | object | None | Any
AcceptedColumnDataType = Dict[str, Any] | int | float | str | bool


class HandlePandasBatchFunction[
    TParameters: AcceptedParametersType,
    TInputColumnDataType: AcceptedColumnDataType,
    TOutputColumnDataType: AcceptedColumnDataType,
](Protocol):
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
        run_context: AsyncPandasBatchFunctionRunContext,
        input_values: List[TInputColumnDataType],
        parameters: Optional[TParameters],
    ) -> AsyncGenerator[TOutputColumnDataType, None]:
        """
        This function is called with a batch of input values and should return a batch of output values.

        :param input_values: input values as a list of dictionaries
        :param parameters: additional parameters passed to the Pandas UDF
        :return: output values as a list of dictionaries
        """
        ...


"""
This is the type alias for when we are operating on a full dataframe not on specific columns
"""


class HandlePandasDataFrameBatchFunction[TParameters: AcceptedParametersType](
    HandlePandasBatchFunction[TParameters, Dict[str, Any], Dict[str, Any]]
):
    pass


"""
This is the type alias for when we are operating on a single column for type struct, and we want to output a struct
"""


class HandlePandasStructToStructBatchFunction[TParameters: AcceptedParametersType](
    HandlePandasBatchFunction[TParameters, Dict[str, Any], Dict[str, Any]]
):
    pass


"""
This is the type alias for when we are operating on a single column for type scalar, and we want to output a struct
"""


class HandlePandasScalarToStructBatchFunction[TParameters: AcceptedParametersType](
    HandlePandasBatchFunction[TParameters, Union[int, float, str, bool], Dict[str, Any]]
):
    pass


"""
This is the type alias for when we are operating on a single column for type scalar, and we want to output a struct
"""


class HandlePandasScalarToScalarBatchFunction[TParameters: AcceptedParametersType](
    HandlePandasBatchFunction[
        TParameters, Union[int, float, str, bool], Union[int, float, str, bool]
    ]
):
    pass


"""
This is the type alias for when we are operating on a single column for type struct, and we want to output a scalar
"""


class HandlePandasStructToScalarBatchFunction[TParameters: AcceptedParametersType](
    HandlePandasBatchFunction[TParameters, Dict[str, Any], Union[int, float, str, bool]]
):
    pass
