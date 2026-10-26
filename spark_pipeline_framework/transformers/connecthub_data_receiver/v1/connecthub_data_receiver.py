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
import math
from bson.json_util import dumps

# parallelize() ships its data inside each task message, so every slice has to stay well under
# spark.rpc.message.maxSize (128 MB by default)
TARGET_SLICE_SIZE_BYTES = 32 * 1024 * 1024
# size at which a staging file is closed and a new part file started
TARGET_STAGING_FILE_SIZE_BYTES = 64 * 1024 * 1024
# size of each chunk handed to the JVM output stream through py4j
STAGING_WRITE_CHUNK_SIZE_BYTES = 8 * 1024 * 1024


class StagingFileWriter:
    """
    Writes JSON lines to part files under a folder through the Hadoop FileSystem API, so the same code works for
    local paths and s3a:// paths and uses the same credentials Spark uses to read the folder back.
    """

    def __init__(self, df: DataFrame, staging_path: str) -> None:
        jvm = df.sparkSession._jvm
        self.hadoop_path_class = jvm.org.apache.hadoop.fs.Path
        self.staging_path = staging_path.rstrip("/")
        root = self.hadoop_path_class(self.staging_path)
        self.fs = root.getFileSystem(df.sparkSession._jsc.hadoopConfiguration())
        # clear out anything left from a previous run with the same path
        self.fs.delete(root, True)
        self.part_number = 0
        self.buffer: List[bytes] = []
        self.buffer_size = 0

    def write(self, rows: List[str]) -> None:
        for row in rows:
            line = (row + "\n").encode("utf-8")
            self.buffer.append(line)
            self.buffer_size += len(line)
            if self.buffer_size >= TARGET_STAGING_FILE_SIZE_BYTES:
                self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        path = self.hadoop_path_class(
            f"{self.staging_path}/part-{self.part_number:05d}.json"
        )
        out = self.fs.create(path, True)
        try:
            chunk: List[bytes] = []
            chunk_size = 0
            for line in self.buffer:
                chunk.append(line)
                chunk_size += len(line)
                if chunk_size >= STAGING_WRITE_CHUNK_SIZE_BYTES:
                    out.write(bytearray(b"".join(chunk)))
                    chunk, chunk_size = [], 0
            if chunk:
                out.write(bytearray(b"".join(chunk)))
        finally:
            out.close()
        self.part_number += 1
        self.buffer, self.buffer_size = [], 0


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
        query_parameters=Optional[Dict[str, Any]],
        staging_path: Optional[str] = None,
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
        :param query_parameters: Dict of key, value pairs which respects document query
        :param staging_path: (Optional) folder (local or s3a://) to write the documents to as JSON lines. Spark then
        reads the folder, so the data never travels inside task messages. Anything already in the folder is deleted.
        If not set, the documents are loaded with parallelize(), sliced so no task exceeds spark.rpc.message.maxSize.
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

        self.query_parameters: Param[Optional[Dict[str, Any]]] = Param(
            self, "query_parameters", ""
        )
        self._setDefault(query_parameters=query_parameters)

        self.db_name: Param[str] = Param(self, "db_name", "")
        self._setDefault(db_name=db_name)

        self.staging_path: Param[Optional[str]] = Param(self, "staging_path", "")
        self._setDefault(staging_path=staging_path)

    def _transform(self, df: DataFrame) -> DataFrame:
        view_name = self.get_view_name()
        conn_string = self.get_conn_string()
        db_name = self.get_db_name()
        query_parameters = self.get_query_parameters()
        staging_path = self.get_staging_path()
        client = pymongo.MongoClient(conn_string)  # type: ignore

        try:
            self.logger.info(f"connected server info: {client.server_info()}")

            integration_hub_db = client[db_name]
            client_connection = integration_hub_db.client_connection

            self.logger.info(
                f"db name: {db_name} and total # of documents in client_connection collection: "
                f"{client_connection.count_documents({})}"
            )

            staging_writer = (
                StagingFileWriter(df=df, staging_path=staging_path)
                if staging_path
                else None
            )
            converted_data: List[str] = []
            total_documents = 0
            last_seen = None
            while True:
                data, last_seen = self.pagination(
                    client_connection, query_parameters, last_seen
                )
                if not data:
                    break
                rows = [json.dumps(r) for r in data]
                total_documents += len(rows)
                if staging_writer:
                    staging_writer.write(rows)
                else:
                    converted_data.extend(rows)

            self.logger.info(
                f"the total number of documents about to be loaded into Spark: {total_documents}"
            )

            if staging_writer and total_documents > 0:
                staging_writer.flush()
                self.logger.info(
                    f"wrote {staging_writer.part_number} staging file(s) to {staging_path}"
                )
                df2 = df.sparkSession.read.json(staging_path)
            else:
                df2 = self.read_json_from_driver(df=df, rows=converted_data)
            df2.createOrReplaceTempView(view_name)

        finally:
            client.close()

        return df

    def read_json_from_driver(self, df: DataFrame, rows: List[str]) -> DataFrame:
        """
        Loads JSON rows held on the driver with parallelize(). The number of slices comes from the data size, not
        just defaultParallelism: defaultParallelism is the number of executor cores registered at this moment, so
        when executors start slowly the slices can exceed spark.rpc.message.maxSize and the job fails.
        """
        total_bytes = sum(len(row.encode("utf-8")) for row in rows)
        num_slices = max(
            sc(df).defaultParallelism,
            math.ceil(total_bytes / TARGET_SLICE_SIZE_BYTES),
        )
        self.logger.info(
            f"loading {len(rows)} documents ({total_bytes} bytes) with parallelize in {num_slices} slices"
        )
        return df.sparkSession.read.json(sc(df).parallelize(rows, numSlices=num_slices))

    def pagination(
        self, client: Any, query_parameters: Optional[Dict[str, Any]], last_seen  # type: ignore
    ) -> Tuple[Union[List[Any], None], Union[datetime, None]]:
        """
        Helper function to paginate results from ConnectHub. The built-in cursor.skip and cursor.limit methods
        have performance problems with large datasets, since all the skipped documents need to be read anyway.

        :param client: PyMongo client for connecting to 'client_connection' collection in 'integration_hub' db.
        :param query_parameters: Dict of key,value pairs which respects document query
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

        # When it is first page
        cursor = (
            client.find(
                {
                    LAST_UPDATED_ON_DATE: (
                        {"$gt": last_seen} if last_seen else {"$gte": last_run_date}
                    ),
                    **(query_parameters if query_parameters else {}),
                },
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

        # this appears to be unique, since it takes more than a ms between updates to generate this timestamp.
        # Read it from the raw document: the extended JSON string drops the milliseconds when they are .000,
        # so parsing it with a fixed format fails for those documents
        last_seen_date: datetime = list_cur[-1][LAST_UPDATED_ON_DATE]

        self.logger.info(
            f"the {LAST_UPDATED_ON_DATE} of the last returned document: {last_seen_date.isoformat()}"
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

    def get_query_parameters(self) -> Optional[Dict[str, Any]]:
        return self.getOrDefault(self.query_parameters)

    def get_staging_path(self) -> Optional[str]:
        return self.getOrDefault(self.staging_path)
