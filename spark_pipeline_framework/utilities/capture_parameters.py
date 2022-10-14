from functools import wraps
from typing import Any, Callable, TypeVar, cast

F = TypeVar("F", bound=Callable)  # type: ignore


def capture_parameters(func: F) -> F:
    """
    A decorator that forces keyword arguments in the wrapped method
    and saves actual input keyword arguments in `_input_kwargs`.

    Notes
    -----
    Should only be used to wrap a method where first arg is `self`
    """

    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if len(args) > 0:
            raise TypeError("Method %s forces keyword arguments." % func.__name__)
        if hasattr(self, "_input_kwargs"):
            self._input_kwargs.update(
                kwargs
            )  # add to existing since this is being used in a subclass
        else:
            self._input_kwargs = kwargs
        return func(self, **kwargs)

    return cast(F, wrapper)
