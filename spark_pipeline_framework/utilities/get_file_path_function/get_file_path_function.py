from typing import Optional
from typing_extensions import Protocol


class GetFilePathFunction(Protocol):
    """
    Type declaration for a function that takes in a view, a resource_name and a loop_id and returns a str

    """

    def __call__(
        self,
        *,
        view: Optional[str] = None,
        resource_name: Optional[str] = None,
        loop_id: Optional[str] = None,
    ) -> str:
        ...
