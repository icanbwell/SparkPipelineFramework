from typing import Dict, Any

from library.features.carriers_python.v1.features_carriers_python_v1 import (
    FeaturesCarriersPythonV1,
)

from library.features.carriers.v1.features_carriers_v1 import FeaturesCarriersV1

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger

from spark_pipeline_framework.pipelines.framework_pipeline import FrameworkPipeline
from spark_pipeline_framework.transformers.framework_csv_loader.v1.framework_csv_loader import (
    FrameworkCsvLoader,
)


class MyPipeline(FrameworkPipeline):
    def __init__(self, parameters: Dict[str, Any], progress_logger: ProgressLogger):
        super(MyPipeline, self).__init__(
            parameters=parameters, progress_logger=progress_logger
        )
        self.transformers = self.create_steps(
            [
                FrameworkCsvLoader(
                    view="flights",
                    filepath=parameters["flights_path"],
                    parameters=parameters,
                    progress_logger=progress_logger,
                ),
                FeaturesCarriersV1(
                    parameters=parameters, progress_logger=progress_logger
                ),
                FeaturesCarriersPythonV1(
                    parameters=parameters, progress_logger=progress_logger
                ),
            ]
        )
