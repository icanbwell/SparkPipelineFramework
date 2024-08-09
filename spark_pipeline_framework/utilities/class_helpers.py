from importlib import import_module
from inspect import signature
from typing import Any, Dict, Optional, Type

from pyspark.ml.base import Transformer


class ClassHelpers:
    @staticmethod
    def get_first_class_in_file(full_reference: str) -> Type[Transformer]:
        module = import_module(full_reference)
        md = module.__dict__
        # find the first class in that module (we assume the first class is the Transformer class)
        my_class: Type[Transformer] = [
            md[c]
            for c in md
            if (isinstance(md[c], type) and md[c].__module__ == module.__name__)
        ][0]
        return my_class

    @staticmethod
    def instantiate_class_with_parameters(
        class_parameters: Dict[str, Any], my_class: Type[Any]
    ) -> Any:
        # find the signature of the __init__ method
        my_class_signature = signature(my_class.__init__)
        my_class_args = [
            param.name
            for param in my_class_signature.parameters.values()
            if param.name != "self"
        ]
        # instantiate the class passing in the parameters + progress_logger
        if len(my_class_args) > 0 and len(class_parameters) > 0:
            # noinspection PyArgumentList
            my_instance = my_class(
                **{k: v for k, v in class_parameters.items() if k in my_class_args}
            )
        else:
            my_instance = my_class()
        return my_instance

    @staticmethod
    def get_full_name_of_instance(o: Any) -> str:
        klass = o.__class__
        module = str(klass.__module__)
        if module == "builtins":
            return str(klass.__qualname__)  # avoid outputs like 'builtins.str'
        return module + "." + str(klass.__qualname__)

    @staticmethod
    def get_function_as_text(fn: Any, strip: Optional[str]) -> str:
        import inspect

        result: str = "\n".join(
            [line.strip() for line in inspect.getsourcelines(fn)[0]]
        )
        if strip and result.startswith(strip):
            result = result[len(strip) :]
        return result

    @staticmethod
    def get_calling_function_name() -> str:
        import inspect

        stack = inspect.stack()
        # stack[0] is this function, stack[1] is its caller, stack[2] is the caller of its caller
        caller_frame = stack[2]
        return caller_frame.function
