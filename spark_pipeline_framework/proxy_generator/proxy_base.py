import os
from os import listdir, path
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

# noinspection PyPackageRequirements
from pyspark.ml.base import Transformer

# noinspection PyPackageRequirements
from pyspark.sql import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.python_transformer_helpers import (
    get_python_transformer_from_location,
    get_python_function_from_location,
)
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner import (
    FrameworkMappingLoader,
)
from spark_pipeline_framework.transformers.framework_sql_transformer.v1.framework_sql_transformer import (
    FrameworkSqlTransformer,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class ProxyBase(FrameworkTransformer):
    def __init__(
        self,
        parameters: Dict[str, Any],
        location: Union[str, Path],
        progress_logger: Optional[ProgressLogger] = None,
        verify_count_remains_same: bool = False,
        files_to_skip: Optional[List[str]] = None,
    ) -> None:
        super().__init__(
            name=self.__class__.__name__,
            parameters=parameters,
            progress_logger=progress_logger,
        )
        self.verify_count_remains_same: bool = verify_count_remains_same
        self.location: str = str(location)
        self.my_transformers: List[Transformer] = []
        self.logger = get_logger(__name__)

        assert self.location
        # Iterate over files to create transformers
        files: List[str] = sorted(listdir(self.location))
        index_of_module: int = self.location.rfind("/library/")
        module_ = index_of_module + 1
        module_name: str = self.location[module_:].replace("/", ".")
        files_to_skip = files_to_skip or []

        # noinspection Mypy
        self.setParams(
            name=self.__class__.__name__,
            parameters=parameters,
            progress_logger=progress_logger,
        )

        for file in files:
            if file not in files_to_skip:
                if file.endswith(".csv"):
                    file_name = file.replace(".csv", "")
                    self.my_transformers.append(
                        FrameworkCsvLoader(
                            view=file_name,
                            file_path=path.join(self.location, file),
                            delimiter=parameters.get("delimiter", ","),
                            has_header=parameters.get("has_header", True),
                            mapping_file_name=file,
                        )
                    )
                elif file.endswith(".sql"):
                    feature_sql: str = self.read_file_as_string(
                        path.join(self.location, file)
                    ).format(parameters=parameters)
                    self.my_transformers.append(
                        FrameworkSqlTransformer(
                            sql=feature_sql,
                            name=module_name,
                            progress_logger=progress_logger,
                            log_sql=parameters.get("debug_log_sql", False),
                            view=file.replace(".sql", ""),
                            verify_count_remains_same=verify_count_remains_same,
                            mapping_file_name=file,
                        )
                    )
                elif file.endswith("mapping.py"):
                    file_name_only: str = os.path.basename(file)
                    # strip off .py to get the module name
                    import_module_name: str = file_name_only.replace(".py", "")
                    self.my_transformers.append(
                        self.get_python_mapping_transformer(
                            "." + import_module_name, file
                        )
                    )
                elif file.endswith("calculate.py") or file.endswith("pipeline.py"):
                    file_name_only = file.replace(".py", "")
                    self.my_transformers.append(
                        self.get_python_transformer(f".{file_name_only}", file)
                    )
            else:
                self.logger.info(f"Skipping processing for file or directory {file}")

        assert len(self.my_transformers) > 0, (
            f"No transformer files found in {self.location}."
            "  There should be one or more .sql, .csv, *mapping.py, *calculate.py or *pipeline.py files"
        )

    @staticmethod
    def read_file_as_string(file_path: str) -> str:
        with open(file_path, "r") as file:
            file_contents = file.read()
        return file_contents

    @property
    def transformers(self) -> List[Transformer]:
        return [
            transformer
            for transformer in self.my_transformers
            if transformer is not None
        ]

    @transformers.setter
    def transformers(self, value: Any) -> None:
        raise AttributeError("transformers property is read only.")

    def _transform(self, df: DataFrame) -> DataFrame:
        # iterate through my transformers
        transformer: Transformer
        i: int = 0
        count: int = len(self.my_transformers)
        for transformer in self.my_transformers:
            if hasattr(transformer, "getName"):
                # noinspection Mypy
                stage_name = transformer.getName()
            else:
                stage_name = transformer.__class__.__name__
            i += 1
            self.logger.info(
                f"---- Running mapping transformer {i} of {count} [{stage_name}]  "
            )

            df = transformer.transform(df)
        return df

    # noinspection PyUnusedLocal
    def fit(self, df: DataFrame) -> Transformer:
        return self

    def get_python_transformer(
        self, import_module_name: str, mapping_file_name: Optional[str] = None
    ) -> Transformer:
        progress_logger = self.getProgressLogger()
        assert progress_logger
        return get_python_transformer_from_location(
            location=self.location,
            import_module_name=import_module_name,
            parameters=self.getParameters() or {},
            progress_logger=progress_logger,
            mapping_file_name=mapping_file_name,
        )

    def get_python_mapping_transformer(
        self, import_module_name: str, mapping_file_name: Optional[str]
    ) -> Transformer:
        parameters: Optional[Dict[str, Any]] = self.getParameters()
        return FrameworkMappingLoader(
            view=parameters["view"] if parameters and "view" in parameters else "",
            mapping_function=get_python_function_from_location(
                location=self.location,
                import_module_name=import_module_name,
                function_name="mapping",
            ),
            parameters=parameters,
            progress_logger=self.getProgressLogger(),
            mapping_file_name=mapping_file_name,
        )

    def as_dict(self) -> Dict[str, Any]:
        my_transformers: List[Transformer] = self.transformers
        if len(my_transformers) == 1 and hasattr(my_transformers[0], "as_dict"):
            return my_transformers[0].as_dict()  # type: ignore
        else:
            return {
                "transformers": [
                    t.as_dict() for t in my_transformers if hasattr(t, "as_dict")
                ]
            }
