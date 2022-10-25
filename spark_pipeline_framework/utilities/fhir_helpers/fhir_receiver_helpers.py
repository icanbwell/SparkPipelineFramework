from datetime import datetime
from logging import Logger
from typing import Union, List, Optional, Dict, Any

from helix_fhir_client_sdk.fhir_client import FhirClient
from helix_fhir_client_sdk.responses.fhir_get_response import FhirGetResponse
from helix_fhir_client_sdk.filters.sort_field import SortField

from spark_pipeline_framework.utilities.fhir_helpers.get_fhir_client import (
    get_fhir_client,
)


def send_fhir_request(
    logger: Logger,
    action: Optional[str],
    action_payload: Optional[Dict[str, Any]],
    additional_parameters: Optional[List[str]],
    filter_by_resource: Optional[str],
    filter_parameter: Optional[str],
    resource_name: str,
    resource_id: Optional[Union[List[str], str]],
    server_url: str,
    include_only_properties: Optional[List[str]],
    page_number: Optional[int] = None,
    page_size: Optional[int] = None,
    last_updated_after: Optional[datetime] = None,
    last_updated_before: Optional[datetime] = None,
    sort_fields: Optional[List[SortField]] = None,
    auth_server_url: Optional[str] = None,
    auth_client_id: Optional[str] = None,
    auth_client_secret: Optional[str] = None,
    auth_login_token: Optional[str] = None,
    auth_access_token: Optional[str] = None,
    auth_scopes: Optional[List[str]] = None,
    separate_bundle_resources: bool = False,
    expand_fhir_bundle: bool = True,
    accept_type: Optional[str] = None,
    content_type: Optional[str] = None,
    accept_encoding: Optional[str] = None,
    extra_context_to_return: Optional[Dict[str, Any]] = None,
) -> FhirGetResponse:
    """
    Sends a fhir request to the fhir client sdk


    :param logger:
    :param action:
    :param action_payload:
    :param additional_parameters:
    :param filter_by_resource:
    :param filter_parameter:
    :param resource_name:
    :param resource_id:
    :param server_url:
    :param include_only_properties:
    :param page_number:
    :param page_size:
    :param last_updated_after:
    :param last_updated_before:
    :param sort_fields:
    :param auth_server_url:
    :param auth_client_id:
    :param auth_client_secret:
    :param auth_login_token:
    :param auth_access_token:
    :param auth_scopes:
    :param separate_bundle_resources: separate bundle resources into a dict where each resourceType is a field with an array of results
    :param expand_fhir_bundle: expand the fhir bundle to create a list of resources
    :param accept_type: (Optional) Accept header to use
    :param content_type: (Optional) Content-Type header to use
    :param accept_encoding: (Optional) Accept-encoding header to use
    :param extra_context_to_return: a dict to return with every row (separate_bundle_resources is set) or with FhirGetResponse
    :return:
    :rtype:
    """

    fhir_client: FhirClient = get_fhir_client(
        logger=logger,
        server_url=server_url,
        auth_server_url=auth_server_url,
        auth_client_id=auth_client_id,
        auth_client_secret=auth_client_secret,
        auth_login_token=auth_login_token,
        auth_access_token=auth_access_token,
        auth_scopes=auth_scopes,
    )
    fhir_client = fhir_client.separate_bundle_resources(separate_bundle_resources)
    fhir_client = fhir_client.expand_fhir_bundle(expand_fhir_bundle)
    if accept_type is not None:
        fhir_client = fhir_client.accept(accept_type)
    if content_type is not None:
        fhir_client = fhir_client.content_type(content_type)
    if accept_encoding is not None:
        fhir_client = fhir_client.accept_encoding(accept_encoding)

    fhir_client = fhir_client.resource(resource_name)
    if resource_id:
        fhir_client = fhir_client.id_(resource_id)
        if filter_by_resource:
            fhir_client = fhir_client.filter_by_resource(filter_by_resource)
            if filter_parameter:
                fhir_client = fhir_client.filter_parameter(filter_parameter)
    # add action to url
    if action:
        fhir_client = fhir_client.action(action)
        if action_payload:
            fhir_client = fhir_client.action_payload(action_payload)
    # add a query for just desired properties
    if include_only_properties:
        fhir_client = fhir_client.include_only_properties(
            include_only_properties=include_only_properties
        )
    if page_size and page_number is not None:
        fhir_client = fhir_client.page_size(page_size).page_number(page_number)
    if sort_fields is not None:
        fhir_client = fhir_client.sort_fields(sort_fields)

    if additional_parameters:
        fhir_client = fhir_client.additional_parameters(additional_parameters)

    # have to done here since this arg can be used twice
    if last_updated_before:
        fhir_client = fhir_client.last_updated_before(last_updated_before)
    if last_updated_after:
        fhir_client = fhir_client.last_updated_after(last_updated_after)

    if extra_context_to_return:
        fhir_client = fhir_client.extra_context_to_return(extra_context_to_return)
    return fhir_client.get()
