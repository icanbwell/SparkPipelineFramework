import asyncio
from typing import AsyncGenerator, Protocol


class ParallelFunction[TInput, TOutput](Protocol):
    async def __call__(self, *, row: TInput) -> TOutput:
        """
        Handle a batch of data

        :return: True if successful
        """
        ...


class AsyncParallelProcessor:
    @staticmethod
    async def process_rows_in_parallel[
        TInput, TOutput
    ](
        rows: list[TInput], process_row_fn: ParallelFunction[TInput, TOutput]
    ) -> AsyncGenerator[TOutput, None]:
        """
        Given a list of rows, it calls the process_row_fn for each row in parallel and yields the results


        :param rows: list of rows to process
        :param process_row_fn: function to process each row
        :return: results of processing
        """
        pending = {asyncio.create_task(process_row_fn(row=row)) for row in rows}

        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    yield await task
                except Exception as e:
                    # Handle or re-raise error
                    # logger.error(f"Error processing row: {e}")
                    raise
