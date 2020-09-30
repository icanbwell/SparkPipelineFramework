import re
from importlib import import_module
from inspect import signature
from os import listdir, path
from typing import Optional, List, Dict, Any

from pyspark.ml.base import Transformer

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_csv_loader import FrameworkCsvLoader
from spark_pipeline_framework.transformers.framework_sql_transformer import FrameworkSqlTransformer


class ProxyBase:
    loader: Optional[Transformer] = None
    converter: Optional[Transformer] = None
    feature: Optional[Transformer] = None
    location: Optional[str] = None

    def __init__(self,
                 parameters: Dict[str, Any],
                 location: str,
                 progress_logger: Optional[ProgressLogger] = None,
                 verify_count_remains_same: bool = False
                 ) -> None:
        self.parameters: Dict[str, Any] = parameters
        self.progress_logger: Optional[ProgressLogger] = progress_logger
        self.verify_count_remains_same: bool = verify_count_remains_same
        self.location: str = location

        assert self.location
        # Iterate over files to create transformers
        files: List[str] = listdir(self.location)
        index_of_module: int = self.location.rfind("/library/")
        module_name: str = self.location[index_of_module + 1:].replace("/", ".")

        for file in files:
            if file == 'my_view.sql':
                load_sql: str = self.read_file_as_string(path.join(
                    self.location, file)).format(parameters=parameters)
                self.loader = FrameworkSqlTransformer(
                    sql=load_sql,
                    name=module_name,
                    progress_logger=progress_logger,
                    log_sql=parameters.get("debug_log_sql", False)
                )
            elif file.endswith('.csv') and self.loader is None:
                file_name = file.replace('.csv', '')
                self.loader = FrameworkCsvLoader(view=file_name, path_to_csv=path.join(self.location, file))
            elif file == 'convert.sql':
                convert_sql: str = self.read_file_as_string(path.join(self.location, file)) \
                    .format(parameters=parameters)
                self.converter = FrameworkSqlTransformer(
                    sql=convert_sql, name=module_name,
                    progress_logger=progress_logger,
                    log_sql=parameters.get("debug_log_sql", False)
                )
            elif file.endswith('.sql') and self.loader is None and self.converter is None:
                feature_sql: str = self.read_file_as_string(path.join(self.location, file)) \
                    .format(parameters=parameters)
                self.feature = FrameworkSqlTransformer(
                    sql=feature_sql,
                    name=module_name,
                    progress_logger=progress_logger,
                    log_sql=parameters.get("debug_log_sql", False),
                    view=file.replace('.sql', ''),
                    verify_count_remains_same=verify_count_remains_same
                )
            elif file == 'calculate.py':
                self.feature = self.get_python_transformer('.calculate')

    @staticmethod
    def read_file_as_string(file_path) -> str:
        with open(file_path, 'r') as file:
            file_contents = file.read()
        return file_contents

    @property
    def transformers(self) -> List[Transformer]:
        return [transformer for transformer in [self.loader, self.converter, self.feature] if transformer is not None]

    @transformers.setter
    def transformers(self, value):
        raise AttributeError("transformers property is read only.")

    def __call__(self, parameters, progress_logger, *args, **kwargs):
        self.__init__(parameters, progress_logger)

    def get_python_transformer(self, import_module_name: str) -> Transformer:
        assert self.location
        search = re.search(r'/library/', self.location)
        assert search
        lib_path = self.location[search.start() +
                                 1:].replace('/', '.').replace('', '')
        module = import_module(import_module_name, lib_path)
        md = module.__dict__
        my_class = [md[c] for c in md if (isinstance(md[c], type) and md[c].__module__ == module.__name__)][0]
        my_class_signature = signature(my_class.__init__)
        my_class_args = [param.name for param in my_class_signature.parameters.values() if param.name != 'self']
        if len(my_class_args) > 0 and len(self.parameters) > 0:
            self.parameters['progress_logger'] = self.progress_logger
            return my_class(**{k: v for k, v in self.parameters.items() if k in my_class_args})
        else:
            return my_class()
