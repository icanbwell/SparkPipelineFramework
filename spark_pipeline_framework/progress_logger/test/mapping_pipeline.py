from typing import Dict, Any, Callable, Union, List, cast

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase

from spark_pipeline_framework.pipelines.v2.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.python_transformer_helpers import (
    get_python_function_from_location,
)
from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner import (
    FrameworkMappingLoader,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)


class MappingPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MappingPipeline, self).__init__(
            parameters=parameters,
            progress_logger=progress_logger,
            run_id="987654321",
            client_name="client_bar",
            vendor_name="vendor_bar",
        )
        mapping_function: Callable[
            [Dict[str, Any]], Union[AutoMapperBase, List[AutoMapperBase]]
        ] = get_python_function_from_location(
            location=str(parameters["feature_path"]),
            import_module_name=".mapping",
            function_name="mapping",
        )

        self.transformers = self.create_steps(
            cast(
                List[FrameworkTransformer],
                [
                    FrameworkMappingLoader(
                        view="members",
                        mapping_function=mapping_function,
                        parameters=parameters,
                        progress_logger=progress_logger,
                    )
                ],
            )
        )
