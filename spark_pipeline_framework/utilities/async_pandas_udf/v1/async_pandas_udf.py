# import asyncio
# import pandas as pd
# from typing import Iterator, AsyncIterator, Callable
#
#
# class AsyncPandasUDF:
#     def __init__(self, async_func: Callable[[pd.Series], pd.DataFrame]):
#         self.async_func = async_func
#
#     # noinspection PyMethodMayBeStatic
#     async def to_async_iter(self, sync_iter: Iterator) -> AsyncIterator:
#         for item in sync_iter:
#             yield item
#
#     async def async_apply_process_batch_udf(
#         self, batch_iter: Iterator[pd.Series]
#     ) -> AsyncIterator[pd.DataFrame]:
#         """
#         Apply the custom function `standardize_batch` to the input batch iterator asynchronously.
#         This is an async function that processes the input data in batches.
#
#         :param batch_iter: iterator of batches of input data
#         :return: async iterator of batches of output data
#         """
#         async for batch in self.to_async_iter(batch_iter):
#             yield await self.async_func(batch)
#
#     # noinspection PyMethodMayBeStatic
#     def collect_async_iterator(self, async_iter: AsyncIterator) -> list:
#         return [item async for item in async_iter]
#
#     def apply_process_batch_udf(
#         self, batch_iter: Iterator[pd.Series]
#     ) -> Iterator[pd.DataFrame]:
#         try:
#             loop = asyncio.get_running_loop()
#         except RuntimeError:
#             loop = asyncio.new_event_loop()
#             asyncio.set_event_loop(loop)
#
#         async_iter = self.async_apply_process_batch_udf(batch_iter)
#         async_gen = loop.run_until_complete(self.collect_async_iterator(async_iter))
#         return iter(async_gen)
#
#     def get_udf(self) -> Callable[[Iterator[pd.Series]], Iterator[pd.DataFrame]]:
#         """
#         Returns a Pandas UDF function that can be used in Spark.
#         """
#         return self.apply_process_batch_udf
