import asyncio
from typing import AsyncGenerator, Protocol, List


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
        rows: List[TInput],
        process_row_fn: ParallelFunction[TInput, TOutput],
        max_concurrent_tasks: int,
    ) -> AsyncGenerator[TOutput, None]:
        """
        Given a list of rows, it calls the process_row_fn for each row in parallel and yields the results

        :param rows: list of rows to process
        :param process_row_fn: function to process each row
        :param max_concurrent_tasks: maximum number of tasks to run concurrently
        :return: results of processing
        """
        semaphore = asyncio.Semaphore(max_concurrent_tasks)
        pending = set()
        row_iterator = iter(rows)

        async def process_with_semaphore(*, row1: TInput) -> TOutput:
            async with semaphore:
                return await process_row_fn(row=row1)

        # Initial filling of the pending set up to max_concurrent_tasks
        for _ in range(min(max_concurrent_tasks, len(rows))):
            row = next(row_iterator)
            pending.add(asyncio.create_task(process_with_semaphore(row1=row)))

        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )

                # Process completed tasks
                for task in done:
                    try:
                        yield await task
                    except Exception as e:
                        # Handle or re-raise error
                        # logger.error(f"Error processing row: {e}")
                        raise

                # Add new tasks if there are remaining rows
                try:
                    while len(pending) < max_concurrent_tasks:
                        row = next(row_iterator)
                        pending.add(
                            asyncio.create_task(process_with_semaphore(row1=row))
                        )
                except StopIteration:
                    pass  # No more rows to process

        finally:
            # Cancel any pending tasks if something goes wrong
            for task in pending:
                task.cancel()
