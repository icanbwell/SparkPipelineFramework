# mypy: ignore-errors

from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Union

from pyspark.ml.param import Param
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.logger.yarn_logger import get_logger

from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters

import pymongo
from spark_pipeline_framework.utilities.spark_data_frame_helpers import sc

import json
from bson.json_util import dumps


class ConnectHubDataReceiver(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters
    def __init__(
        self,
        view_name: str,
        conn_string: str,
        db_name: str,
        last_run_date: Optional[datetime] = datetime(1970, 1, 1),
        page_size: Optional[
            int
        ] = 0,  # setting to 0 ignores limit and will return all results
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ) -> None:
        """
        Transformer to call and receive data from ConnectHub (DocumentDB). It is specific to ConnectHub for now,
        but it should/will get refactored into a generic DocumentDbDataReceiver when time allows.

        :param view_name: name of the view to read the response into
        :param conn_string: the root URL
        :param db_name: the name of the database we need to query
        :param last_run_date: (Optional) the datetime which we should be querying from to get the latest updates. It is
        set to 1/1/1970 by default so that we grab all records without filtering (since there doesn't seem to be a good
        automated way to pass this in from pipeline runs at this point)
        :param page_size: (Optional) the number of documents we should be querying at a time, if we want paging.
        Setting to 0 (which is the default) will ignore paging and all documents (within the query parameters) will
        get pulled at once.
        :param name: (Optional) name of transformer
        :param parameters: (Optional) parameters
        :param progress_logger: (Optional) progress logger
        """
        super(ConnectHubDataReceiver, self).__init__(
            name=name,
            parameters=parameters,
            progress_logger=progress_logger,
        )

        self.logger = get_logger(__name__)

        self.view_name: Param[str] = Param(self, "view_name", "")
        self._setDefault(view_name=view_name)

        self.page_size: Param[int] = Param(self, "page_size", "")
        self._setDefault(page_size=page_size)

        self.conn_string: Param[str] = Param(self, "conn_string", "")
        self._setDefault(conn_string=conn_string)

        self.last_run_date: Param[datetime] = Param(self, "last_run_date", "")
        self._setDefault(last_run_date=last_run_date)

        self.db_name: Param[str] = Param(self, "db_name", "")
        self._setDefault(db_name=db_name)

    def _transform(self, df: DataFrame) -> DataFrame:
        view_name = self.get_view_name()
        conn_string = self.get_conn_string()
        db_name = self.get_db_name()

        client = pymongo.MongoClient(conn_string)  # type: ignore

        try:
            self.logger.info(f"connected server info: {client.server_info()}")

            integration_hub_db = client[db_name]
            client_connection = integration_hub_db.client_connection

            self.logger.info(
                f"db name: {db_name} and total # of documents in client_connection collection: "
                f"{client_connection.count_documents({})}"
            )

            converted_data: List[Any] = []
            last_seen = None
            while True:
                data, last_seen = self.pagination(client_connection, last_seen)
                if not data:
                    break
                converted_data.extend(data)

            self.logger.info(
                f"the total number of documents about to be loaded into Spark: {len(converted_data)}"
            )

            df2 = df.sparkSession.read.json(
                sc(df).parallelize([json.dumps(r) for r in converted_data])
            )
            df2.createOrReplaceTempView(view_name)

        finally:
            client.close()

        return df

    def pagination(
        self, client: Any, last_seen  # type: ignore
    ) -> Tuple[Union[List[Any], None], Union[datetime, None]]:
        """
        Helper function to paginate results from ConnectHub. The built-in cursor.skip and cursor.limit methods
        have performance problems with large datasets, since all the skipped documents need to be read anyway.

        :param client: PyMongo client for connecting to 'client_connection' collection in 'integration_hub' db.
        :param last_seen: The latest 'lastUpdatedOnDate' datetime for the last document that was pulled during the
        previous page, so that we can start paging from the next document after that. Note: if page_size is 0, all
        documents will be pulled at the same time without paging.
        :returns: A tuple consisting of the queried data (List) and the datetime representing the
        latest 'lastUpdatedOnDate' datetime for the last document that was pulled during this page.
        """
        page_size = self.get_page_size()
        last_run_date = self.get_last_run_date()

        self.logger.info(f"last_run_date: {last_run_date}; last_seen: {last_seen}")

        LAST_UPDATED_ON_DATE = "lastUpdatedOnDate"

        if last_seen is None:
            # When it is first page
            cursor = (
                client.find(
                    {
                        LAST_UPDATED_ON_DATE: {"$gte": last_run_date},
                    }
                )
                .sort(LAST_UPDATED_ON_DATE, pymongo.ASCENDING)
                .limit(page_size)
            )
        else:
            cursor = (
                client.find(
                    {
                        LAST_UPDATED_ON_DATE: {"$gt": last_seen},
                    }
                )
                .sort(LAST_UPDATED_ON_DATE, pymongo.ASCENDING)
                .limit(page_size)
            )

        # Get the data
        list_cur = list(cursor)

        if not list_cur:
            # No documents left
            return None, None

        json_string = dumps(list_cur)
        data: List[Any] = json.loads(json_string)

        self.logger.info(f"returned # of documents: {len(data)}")

        # this appears to be unique, since it takes more than a ms between updates to generate this timestamp
        last_seen_string = data[-1][LAST_UPDATED_ON_DATE]["$date"]
        last_seen_date = datetime.strptime(last_seen_string, "%Y-%m-%dT%H:%M:%S.%fZ")

        self.logger.info(
            f"the {LAST_UPDATED_ON_DATE} of the last returned document: {last_seen_string}"
        )

        return data, last_seen_date

    def get_view_name(self) -> str:
        return self.getOrDefault(self.view_name)

    def get_page_size(self) -> int:
        return self.getOrDefault(self.page_size)

    def get_conn_string(self) -> str:
        return self.getOrDefault(self.conn_string)

    def get_last_run_date(self) -> datetime:
        return self.getOrDefault(self.last_run_date)

    def get_db_name(self) -> str:
        return self.getOrDefault(self.db_name)
