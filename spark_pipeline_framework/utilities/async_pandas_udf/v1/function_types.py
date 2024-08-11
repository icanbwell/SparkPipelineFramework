from typing import Protocol, List, Dict, Any, Optional, TypeVar, AsyncGenerator


# By declaring T as contravariant=True, you indicate that T can be a more general type than the actual type used,
# which aligns with how Protocol expects type variables to be used in this context.
T = TypeVar("T", contravariant=True)


class HandlePandasBatchFunction(Protocol[T]):
    async def __call__(
        self,
        *,
        partition_index: int,
        chunk_index: int,
        input_values: List[Dict[str, Any]],
        parameters: Optional[T],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        This function is called with a batch of input values and should return a batch of output values.

        :param partition_index: The index of the partition.
        :param chunk_index: The index of the chunk within the partition.
        :param input_values: input values as a list of dictionaries
        :param parameters: additional parameters passed to the Pandas UDF
        :return: output values as a list of dictionaries
        """
        ...


class HandlePandasBatchWithParametersFunction(Protocol[T]):
    async def __call__(
        self,
        *,
        partition_index: int,
        chunk_index: int,
        input_values: List[Dict[str, Any]],
        parameters: Optional[T],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        This function is called with a batch of input values and should return a batch of output values.


        :param partition_index: The index of the partition.
        :param chunk_index: The index of the chunk within the partition.
        :param input_values: input values as a list of dictionaries
        :param parameters: additional parameters passed to the Pandas UDF
        :return: output values as a list of dictionaries
        """
        ...
