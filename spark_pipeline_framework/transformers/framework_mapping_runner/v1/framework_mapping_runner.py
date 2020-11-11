from typing import Dict, Any, Callable, Optional, Union, List

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.param import Param
from pyspark.sql import DataFrame
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.automappers.automapper_base import AutoMapperBase

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import FrameworkTransformer


class FrameworkMappingLoader(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        view: str,
        mapping_function: Callable[[Dict[str, Any]],
                                   Union[AutoMapperBase,
                                         List[AutoMapperBase]]],
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None
    ) -> None:
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        self.view: Param = Param(self, "view", "")
        # noinspection Mypy
        self._setDefault(view=None)

        self.mapping_function: Callable[[Dict[str, Any]], Union[
            AutoMapperBase, List[AutoMapperBase]]] = mapping_function

        # noinspection Mypy
        kwargs = self._input_kwargs
        # remove mapping_function since that is not serializable
        kwargs = {
            key: value
            for key, value in kwargs.items() if key != "mapping_function"
        }
        self.setParams(**kwargs)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring, PyUnusedLocal,Mypy
    @keyword_only
    def setParams(
        self,
        view: str,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None
    ) -> Any:
        # noinspection Mypy
        kwargs = self._input_kwargs
        super().setParams(
            name=name, parameters=parameters, progress_logger=progress_logger
        )
        # noinspection Mypy
        return self._set(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        view: str = self.getView()
        mapping_function: Callable[[Dict[str, Any]], Union[
            AutoMapperBase, List[AutoMapperBase]]] = self.getMappingFunction()

        # run the mapping function to get an AutoMapper
        incoming_parameters: Optional[Dict[str, Any]] = self.getParameters()
        parameters: Dict[
            str,
            Any] = incoming_parameters.copy() if incoming_parameters else {}
        parameters["view"] = view

        auto_mapper: Union[
            AutoMapperBase,
            List[AutoMapperBase]] = mapping_function(parameters)

        assert isinstance(auto_mapper, AutoMapper) or (auto_mapper, list)

        if isinstance(auto_mapper, AutoMapper):
            # then call transform() on the AutoMapper
            df = auto_mapper.transform(df=df)
            # create the view
            df.createOrReplaceTempView(view)
        elif isinstance(auto_mapper, list):
            auto_mappers: List[AutoMapperBase] = auto_mapper
            for a in auto_mappers:
                assert isinstance(a, AutoMapper)
                a.transform(df=df)

        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getView(self) -> str:
        return self.getOrDefault(self.view)  # type: ignore

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getMappingFunction(
        self
    ) -> Callable[[Dict[str, Any]], Union[AutoMapperBase,
                                          List[AutoMapperBase]]]:
        return self.mapping_function
