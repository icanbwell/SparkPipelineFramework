from pathlib import Path
from typing import Dict, Any, cast, List

from pyspark.ml import Transformer

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1
from library.features.carriers_fhir.v1.features_carriers_fhir_v1 import (
    FeaturesCarriersFhirV1,
)
from library.features.carriers_python.v1.features_carriers_python_v1 import (
    FeaturesCarriersPythonV1,
)
from spark_pipeline_framework.pipelines.v2.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
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
from spark_pipeline_framework.transformers.send_automapper_to_fhir.v1.automapper_to_fhir_transformer import (
    AutoMapperToFhirTransformer,
)


class SimplePipeline(FrameworkPipeline):
    def get_fhir_path(self, view: str, resource_name: str) -> str:
        return str(
            self.data_dir.joinpath("temp")
            .joinpath("output")
            .joinpath(f"{resource_name}-{view}")
        )

    def get_fhir_response_path(self, view: str, resource_name: str) -> str:
        return str(
            self.data_dir.joinpath("temp")
            .joinpath("output")
            .joinpath(f"{resource_name}-{view}-response")
        )

    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(SimplePipeline, self).__init__(
            parameters=parameters,
            progress_logger=progress_logger,
            run_id="12345678",
            client_name="client_foo",
            vendor_name="vendor_foo",
        )
        self.data_dir: Path = Path(__file__).parent.joinpath("./")

        self.transformers = self.create_steps(
            cast(
                List[Transformer],
                [
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
                    AutoMapperToFhirTransformer(
                        name="AutoMapperToFhirTransformer",
                        parameters=parameters,
                        progress_logger=progress_logger,
                        transformer=FeaturesCarriersFhirV1(
                            parameters=parameters, progress_logger=progress_logger
                        ),
                        func_get_path=self.get_fhir_path,
                        func_get_response_path=self.get_fhir_response_path,
                        fhir_server_url="http://mock-server:1080",
                        source_entity_name="members",
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
        )
