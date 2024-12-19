from typing import Dict, Any, Callable, Union, List

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1
from library.features.carriers_python.v1.features_carriers_python_v1 import (
    FeaturesCarriersPythonV1,
)
from spark_pipeline_framework.pipelines.v2.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.python_transformer_helpers import (
    get_python_function_from_location,
)
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)
from spark_pipeline_framework.transformers.framework_drop_views_transformer.v1.framework_drop_views_transformer import (
    FrameworkDropViewsTransformer,
)
from spark_pipeline_framework.transformers.framework_if_else_transformer.v1.framework_if_else_transformer import (
    FrameworkIfElseTransformer,
)
from spark_pipeline_framework.transformers.framework_json_exporter.v1.framework_json_exporter import (
    FrameworkJsonExporter,
)
from spark_pipeline_framework.transformers.framework_loop_transformer.v1.framework_loop_transformer import (
    FrameworkLoopTransformer,
)
from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner import (
    FrameworkMappingLoader,
)


class LoopingPipeline(FrameworkPipeline):
    def __init__(
        self,
        parameters: Dict[str, Any],
        progress_logger: ProgressLogger,
        max_number_of_runs: int = 1,
    ):
        super(LoopingPipeline, self).__init__(
            parameters=parameters,
            progress_logger=progress_logger,
            run_id="87654321",
            client_name="client_foo",
            vendor_name="vendor_foo",
        )

        mapping_function: Callable[
            [Dict[str, Any]], Union[AutoMapperBase, List[AutoMapperBase]]
        ] = get_python_function_from_location(
            location=str(parameters["feature_path"]),
            import_module_name=".mapping",
            function_name="mapping",
        )

        self.steps = [
            FrameworkLoopTransformer(
                name="FrameworkLoopTransformer",
                parameters=parameters,
                progress_logger=progress_logger,
                sleep_interval_in_seconds=2,
                max_number_of_runs=max_number_of_runs,
                stages=[
                    FrameworkDropViewsTransformer(
                        name="",
                        parameters=parameters,
                        progress_logger=progress_logger,
                        views=["foo"],
                    ),
                    FrameworkCsvLoader(
                        name="FrameworkCsvLoader",
                        view="flights",
                        file_path=parameters["flights_path"],
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                    FeaturesCarriersV1(
                        parameters=parameters, progress_logger=progress_logger
                    ),
                    FrameworkIfElseTransformer(
                        enable=True,
                        progress_logger=progress_logger,
                        stages=[
                            FeaturesCarriersPythonV1(
                                parameters=parameters, progress_logger=progress_logger
                            ),
                        ],
                    ),
                    FrameworkMappingLoader(
                        view="members",
                        mapping_function=mapping_function,
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                    FrameworkJsonExporter(
                        file_path=parameters["export_path"],
                        view="flights",
                        name="FrameworkJsonExporter",
                        parameters=parameters,
                        progress_logger=progress_logger,
                    ),
                ],
            )
        ]
