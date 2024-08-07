from typing import Protocol, List, Dict, Any, Optional, TypeVar, AsyncGenerator


class HandlePandasBatchFunction(Protocol):
    async def __call__(
        self,
        input_values: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        This function is called with a batch of input values and should return a batch of output values.

        :param input_values: input values as a list of dictionaries
        :return: output values as a list of dictionaries
        """
        ...


# By declaring T as contravariant=True, you indicate that T can be a more general type than the actual type used,
# which aligns with how Protocol expects type variables to be used in this context.
T = TypeVar("T", contravariant=True)


class HandlePandasBatchWithParametersFunction(Protocol[T]):
    async def __call__(
        self, input_values: List[Dict[str, Any]], parameters: Optional[T]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        This function is called with a batch of input values and should return a batch of output values.

        :param input_values: input values as a list of dictionaries
        :param parameters: additional parameters
        :return: output values as a list of dictionaries
        """
        ...
