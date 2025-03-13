from typing import AsyncGenerator, List, Optional

from helixcore.utilities.async_helper.v1.async_helper import AsyncHelper
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class AsyncDataFrameHelper:
    @staticmethod
    async def async_generator_to_dataframe[T](
        df: DataFrame,
        async_gen: AsyncGenerator[T, None],
        schema: StructType,
        results_per_batch: Optional[int],
    ) -> DataFrame:
        """
        Takes an async generator, adds it to the dataframe in batches of size `results_per_batch`
        and returns the dataframe

        :param df: DataFrame
        :param async_gen: AsyncGenerator
        :param schema: StructType | AtomicType
        :param results_per_batch: int
        :return: DataFrame
        """

        # iterate through the async generator and collect the data in chunks
        collected_data_frames: List[DataFrame] = []
        if results_per_batch is not None:
            # if batching is requested
            async for chunk in AsyncHelper.collect_async_data(
                async_gen=async_gen, chunk_size=results_per_batch
            ):
                df_chunk = df.sparkSession.createDataFrame(chunk, schema)  # type: ignore[type-var]
                collected_data_frames.append(df_chunk)

            # Combine all chunks into a single DataFrame
            if len(collected_data_frames) > 0:
                # if data was already collected, combine it into a single DataFrame
                final_df = collected_data_frames[0]
                for df in collected_data_frames[1:]:
                    final_df = final_df.union(df)
                return final_df
            else:
                # create a DataFrame with the schema but no data
                return df.sparkSession.createDataFrame([], schema)
        else:
            # no batching required
            collected_data: List[T] = []
            async for item in async_gen:
                collected_data.append(item)
            return df.sparkSession.createDataFrame(collected_data, schema)  # type: ignore[type-var]
