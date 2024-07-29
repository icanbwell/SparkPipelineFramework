from typing import Any, Callable, Dict, List, Optional, Union

# noinspection PyProtectedMember,PyPackageRequirements
from mlflow.entities import RunStatus  # type: ignore
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml.param import Param
from pyspark.sql import DataFrame
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.automappers.automapper_base import AutoMapperBase

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner_exception import (
    FrameworkMappingRunnerException,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)

# define type for AutoMapperFunction
AutoMapperTypeOrList = Union[AutoMapperBase, List[AutoMapperBase]]
AutoMapperFunction = Callable[[Dict[str, Any]], AutoMapperTypeOrList]


class FrameworkMappingLoader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view: str,
        mapping_function: AutoMapperFunction,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        mapping_file_name: Optional[str] = None,
    ) -> None:
        """
        This class loads AutoMappers and runs them

        :param view:
        :param mapping_function:
        :param name:
        :param parameters:
        :param progress_logger:
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.view: Param[str] = Param(self, "view", "")
        self.views: List[str] = []
        # noinspection Mypy
        self._setDefault(view=None)

        self.mapping_function: AutoMapperFunction = mapping_function

        # noinspection Mypy
        kwargs = self._input_kwargs
        # remove mapping_function since that is not serializable
        kwargs = {
            key: value for key, value in kwargs.items() if key != "mapping_function"
        }
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        mapping_function: AutoMapperFunction = self.getMappingFunction()

        # run the mapping function to get an AutoMapper
        incoming_parameters: Optional[Dict[str, Any]] = self.getParameters()
        parameters: Dict[str, Any] = (
            incoming_parameters.copy() if incoming_parameters else {}
        )
        parameters["view"] = view

        auto_mappers: List[AutoMapperBase] = []
        auto_mapper = mapping_function(parameters)
        if isinstance(auto_mapper, list):
            auto_mappers = auto_mappers + auto_mapper
        else:
            auto_mappers.append(auto_mapper)

        assert isinstance(auto_mappers, list)

        i: int = 0
        count: int = len(auto_mappers)
        for automapper in auto_mappers:
            i += 1
            assert isinstance(automapper, AutoMapper)
            if automapper.view:
                self.views.append(automapper.view)
            self.logger.info(
                f"---- Running AutoMapper {i} of {count} [{automapper}] ----"
            )
            table_names: List[str] = [
                t.name for t in df.sparkSession.catalog.listTables()
            ]

            # if view exists then drop it
            if (
                automapper.view
                and automapper.use_single_select
                and not automapper.reuse_existing_view
                and automapper.view in table_names
            ):
                self.logger.info(f"Dropping view {automapper.view}")
                df.sparkSession.catalog.dropTempView(viewName=automapper.view)

            progress_logger: Optional[ProgressLogger] = self.getProgressLogger()
            try:
                stage_name = automapper.__class__.__name__

                run_name = f"{stage_name} map view - {automapper.view}"

                if progress_logger is not None:
                    progress_logger.start_mlflow_run(run_name=run_name, is_nested=True)

                try:
                    automapper.transform(df=df)
                except Exception as e:
                    if len(e.args) >= 1:
                        e.args = (f"In AutoMapper ({stage_name})", *e.args)
                    raise e

                if progress_logger is not None:
                    progress_logger.end_mlflow_run()
            except Exception as e:
                error_msg = f"Error running automapper {automapper.view}"
                if progress_logger is not None:
                    progress_logger.log_exception(
                        event_name=str(automapper), event_text=error_msg, ex=e
                    )
                    progress_logger.end_mlflow_run(status=RunStatus.FAILED)
                raise FrameworkMappingRunnerException(msg=error_msg, exception=e) from e

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)

    # noinspection PyPep8Naming
    def getViews(self) -> List[str]:
        return self.views

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMappingFunction(self) -> AutoMapperFunction:
        return self.mapping_function

    def __str__(self) -> str:
        return f"{self.__class__.__name__}: {self.getView()}"
