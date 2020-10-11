from pathlib import Path
from typing import Callable, Dict, Any

from pyspark.sql import SparkSession
from spark_auto_mapper.automapper_base import AutoMapperBase
from spark_auto_mapper.automapper_with_column import AutoMapperWithColumn

from spark_pipeline_framework.proxy_generator.python_transformer_helpers import get_python_function_from_location


def test_can_find_python_function(spark_session: SparkSession):
    # Arrange
    data_dir: Path = Path(__file__).parent.joinpath('./')

    # Act
    result_function: Callable[[Dict[str, Any]], AutoMapperBase] = get_python_function_from_location(
        location=str(data_dir.joinpath("library/features/carriers_mapping/v1")),
        import_module_name='.mapping'
    )

    result: AutoMapperBase = result_function({"view": "bar"})

    # Assert
    assert isinstance(result, AutoMapperWithColumn)
