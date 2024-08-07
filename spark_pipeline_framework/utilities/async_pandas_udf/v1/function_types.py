from typing import Protocol, List, Dict, Any


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
