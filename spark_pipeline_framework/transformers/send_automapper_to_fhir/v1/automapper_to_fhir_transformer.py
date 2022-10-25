import json
from typing import Any, Callable, Dict, List, Optional

from spark_pipeline_framework.transformers.athena_table_creator.v1.athena_table_creator import (
    AthenaTableCreator,
)
from spark_pipeline_framework.transformers.fhir_exporter.v1.fhir_exporter import (
    FhirExporter,
)
from spark_pipeline_framework.transformers.fhir_sender.v1.fhir_sender import FhirSender
from spark_pipeline_framework.transformers.send_automapper_to_fhir.exceptions.automapper_to_fhir_transformer_exception import (
    AutomapperToFhirTransformerException,
)
from spark_pipeline_framework.utilities.athena.athena_source_file_type import (
    AthenaSourceFileType,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters
from pyspark.ml import Transformer
from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.proxy_generator.proxy_base import ProxyBase
from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner import (
    FrameworkMappingLoader,
)
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.spark_data_frame_helpers import (
    spark_is_data_frame_empty,
)


class AutoMapperToFhirTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        # add your parameters here (be sure to add them to setParams below too)
        transformer: ProxyBase,
        func_get_path: Callable[[str, str], str],
        func_get_response_path: Callable[[str, str], str],
        fhir_server_url: str,
        source_entity_name: str,
        fhir_validation_url: Optional[str] = None,
        athena_schema: Optional[str] = None,
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        auth_server_url: Optional[str] = None,
        auth_client_id: Optional[str] = None,
        auth_client_secret: Optional[str] = None,
        auth_login_token: Optional[str] = None,
        auth_scopes: Optional[List[str]] = None,
        progress_logger: Optional[ProgressLogger] = None,
        send_to_fhir: Optional[bool] = True,
    ):
        """
        Runs the auto-mappers, saves the result to Athena db and then sends the results to fhir server

        :param transformer: transformer to run
        :param func_get_path: function that gets the local path for a resource
        :param func_get_response_path: function that gets the path to save the responses from the FHIR server
        :param fhir_server_url: url to FHIR server
        :param fhir_validation_url: url to FHIR validation server
        :param source_entity_name: resource name
        :param athena_schema: schema to use when writing data to Athena
        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        assert fhir_server_url

        self.logger = get_logger(__name__)
        self.logger.info(
            f"Parameters={json.dumps({str(key): str(value) for key, value in parameters.items()}) if parameters else 'None'}"
        )

        # add a param
        self.transformer: Param[ProxyBase] = Param(self, "transformer", "")
        self._setDefault(transformer=transformer)

        self.func_get_path: Param[Callable[[str, str], str]] = Param(
            self, "func_get_path", ""
        )
        self._setDefault(func_get_path=func_get_path)

        self.func_get_response_path: Param[Callable[[str, str], str]] = Param(
            self, "func_get_response_path", ""
        )
        self._setDefault(func_get_response_path=func_get_response_path)

        self.fhir_server_url: Param[str] = Param(self, "fhir_server_url", "")
        self._setDefault(fhir_server_url=fhir_server_url)

        self.fhir_validation_url: Param[Optional[str]] = Param(
            self, "fhir_validation_url", ""
        )
        self._setDefault(fhir_validation_url=fhir_validation_url)

        self.source_entity_name: Param[str] = Param(self, "source_entity_name", "")
        self._setDefault(source_entity_name=source_entity_name)

        self.athena_schema: Param[Optional[str]] = Param(self, "athena_schema", "")
        self._setDefault(athena_schema=athena_schema)

        self.auth_server_url: Param[Optional[str]] = Param(self, "auth_server_url", "")
        self._setDefault(auth_server_url=None)

        self.auth_client_id: Param[Optional[str]] = Param(self, "auth_client_id", "")
        self._setDefault(auth_client_id=None)

        self.auth_client_secret: Param[Optional[str]] = Param(
            self, "auth_client_secret", ""
        )
        self._setDefault(auth_client_secret=None)

        self.auth_login_token: Param[Optional[str]] = Param(
            self, "auth_login_token", ""
        )
        self._setDefault(auth_login_token=None)

        self.auth_scopes: Param[Optional[List[str]]] = Param(self, "auth_scopes", "")
        self._setDefault(auth_scopes=None)

        self.send_to_fhir: Param[Optional[bool]] = Param(self, "send_to_fhir", "")
        self._setDefault(send_to_fhir=send_to_fhir)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        transformer: ProxyBase = self.getTransformer()
        func_get_path: Callable[[str, str], str] = self.getFuncGetPath()
        func_get_response_path: Callable[
            [str, str], str
        ] = self.getFuncGetResponsePath()
        fhir_server_url: str = self.getFhirServerUrl()
        fhir_validation_url: Optional[str] = self.getFhirValidationServerUrl()
        name: Optional[str] = self.getName()
        source_entity_name: str = self.getSourceEntityName()
        athena_schema: Optional[str] = self.getAthenaSchema()
        parameters = self.getParameters()
        assert parameters
        progress_logger = self.getProgressLogger()

        auth_server_url: Optional[str] = self.getAuthServerUrl()
        auth_client_id: Optional[str] = self.getAuthClientId()
        auth_client_secret: Optional[str] = self.getAuthClientSecret()
        auth_login_token: Optional[str] = self.getAuthLoginToken()
        auth_scopes: Optional[List[str]] = self.getAuthScopes()
        send_to_fhir: Optional[bool] = self.getSendToFhir()

        self.logger.info(f"Calling {fhir_server_url} with client_id={auth_client_id}")

        if len(transformer.transformers) == 0:
            raise AutomapperToFhirTransformerException(
                f"No transformers found for '{transformer.name}'."
            )

        t: Transformer
        for t in transformer.transformers:
            if isinstance(t, FrameworkMappingLoader):
                t.transform(df)  # run the automapper
                views: List[str] = t.getViews()
                view: str
                for view in views:
                    self.logger.info(
                        f"---- Started processing for view: {view} --------"
                    )
                    # get resource name
                    result_df: DataFrame = df.sql_ctx.table(view)
                    if spark_is_data_frame_empty(df=result_df):
                        self.logger.info(f"No data to export/send for view '{view}'")
                        continue
                    self.logger.info(f"----- view: {view} has rows-------")
                    first_row = result_df.select("resourceType").first()
                    assert first_row
                    # noinspection PyPep8Naming
                    resourceType: str = first_row["resourceType"]
                    fhir_resource_path: str = func_get_path(view, resourceType)
                    fhir_resource_response_path: str = func_get_response_path(
                        view, resourceType
                    )
                    # export as FHIR to local fhir_resource_path
                    FhirExporter(
                        view=view,
                        file_path=fhir_resource_path,
                        parameters=parameters,
                        progress_logger=progress_logger,
                        name=f"{name}_fhir_exporter",
                    ).transform(df)
                    if fhir_resource_path.startswith("s3") and athena_schema:
                        # create table in Athena
                        s3_temp_folder: str = str(parameters.get("s3_temp_folder"))
                        AthenaTableCreator(
                            view=view,
                            schema_name=athena_schema,
                            table_name=f"{source_entity_name}_{resourceType}",
                            s3_source_path=fhir_resource_path,
                            source_file_type=AthenaSourceFileType.JSONL,
                            s3_temp_folder=s3_temp_folder,
                            parameters=parameters,
                            progress_logger=progress_logger,
                        ).transform(df)
                    # send to FHIR server
                    if send_to_fhir:
                        FhirSender(
                            resource=resourceType,
                            server_url=fhir_server_url,
                            validation_server_url=fhir_validation_url,
                            file_path=fhir_resource_path,
                            response_path=fhir_resource_response_path,
                            parameters=parameters,
                            progress_logger=progress_logger,
                            batch_size=parameters.get("batch_size", 0),
                            throw_exception_on_validation_failure=parameters.get(
                                "throw_exception_on_validation_failure"
                            ),
                            auth_server_url=auth_server_url,
                            auth_client_id=auth_client_id,
                            auth_client_secret=auth_client_secret,
                            auth_login_token=auth_login_token,
                            auth_scopes=auth_scopes,
                            name=f"{name}_fhir_sender",
                        ).transform(df)
        return df

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getTransformer(self) -> ProxyBase:
        return self.getOrDefault(self.transformer)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFuncGetPath(self) -> Callable[[str, str], str]:
        return self.getOrDefault(self.func_get_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFuncGetResponsePath(self) -> Callable[[str, str], str]:
        return self.getOrDefault(self.func_get_response_path)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFhirServerUrl(self) -> str:
        return self.getOrDefault(self.fhir_server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getFhirValidationServerUrl(self) -> Optional[str]:
        return self.getOrDefault(self.fhir_validation_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSourceEntityName(self) -> str:
        return self.getOrDefault(self.source_entity_name)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAthenaSchema(self) -> Optional[str]:
        return self.getOrDefault(self.athena_schema)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthServerUrl(self) -> Optional[str]:
        return self.getOrDefault(self.auth_server_url)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthClientId(self) -> Optional[str]:
        return self.getOrDefault(self.auth_client_id)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthClientSecret(self) -> Optional[str]:
        return self.getOrDefault(self.auth_client_secret)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthLoginToken(self) -> Optional[str]:
        return self.getOrDefault(self.auth_login_token)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getAuthScopes(self) -> Optional[List[str]]:
        return self.getOrDefault(self.auth_scopes)

    # noinspection PyPep8Naming,PyMissingOrEmptyDocstring
    def getSendToFhir(self) -> Optional[bool]:
        return self.getOrDefault(self.send_to_fhir)
