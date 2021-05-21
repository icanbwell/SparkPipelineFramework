from typing import Optional, Dict, Any

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_file_transformer.v1.framework_file_transformer import (
    FrameworkFileTransformer,
)


class FrameworkS3FileUploader(FrameworkFileTransformer):
    def __init__(
        self,
        name: str = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
