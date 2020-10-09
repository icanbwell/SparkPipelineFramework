import pkgutil
import re
import sys
from importlib import import_module
from inspect import signature
from typing import Dict, Any, Optional

from pyspark.ml import Transformer
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger


def get_python_transformer_from_location(location: str,
                                         import_module_name: str,
                                         parameters: Dict[str, Any],
                                         progress_logger: Optional[ProgressLogger]
                                         ) -> Transformer:
    assert location
    assert isinstance(parameters, dict)
    assert progress_logger
    search = re.search(r'/library/', location)
    assert search
    lib_path = location[search.start() + 1:].replace('/', '.').replace('', '')
    # load_all_modules_from_dir(location)
    module = import_module(import_module_name, lib_path)
    md = module.__dict__
    my_class = [md[c] for c in md if (isinstance(md[c], type) and md[c].__module__ == module.__name__)][0]
    my_class_signature = signature(my_class.__init__)
    my_class_args = [param.name for param in my_class_signature.parameters.values() if param.name != 'self']
    # now figure out the class_parameters to use when instantiating the class
    class_parameters = parameters.copy()
    class_parameters["parameters"] = parameters
    class_parameters['progress_logger'] = progress_logger
    if len(my_class_args) > 0 and len(class_parameters) > 0:
        return my_class(**{k: v for k, v in class_parameters.items() if k in my_class_args})
    else:
        return my_class()


def load_all_modules_from_dir(dirname):
    for importer, package_name, _ in pkgutil.iter_modules([dirname]):
        full_package_name = '%s.%s' % (dirname, package_name)
        if full_package_name not in sys.modules:
            module = importer.find_module(package_name).load_module(full_package_name)
            print(module)
