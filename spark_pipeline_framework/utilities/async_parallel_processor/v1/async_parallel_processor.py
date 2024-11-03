import asyncio
from asyncio import Task
from typing import AsyncGenerator, Protocol, List, Optional, Set, Iterator, Dict, Any


class ParallelFunction[TInput, TOutput, TParameters](Protocol):
    async def __call__(
        self,
        *,
        row: TInput,
        parameters: Optional[TParameters],
        log_level: Optional[str],
        **kwargs: Any,
    ) -> TOutput:
        """
        Handle a batch of data

        :return: True if successful
        """
        ...


class AsyncParallelProcessor:
    @staticmethod
    async def process_rows_in_parallel[
        TInput, TOutput, TParameters: Dict[str, Any] | object
    ](
        rows: List[TInput],
        process_row_fn: ParallelFunction[TInput, TOutput, TParameters],
        max_concurrent_tasks: int,
        parameters: Optional[TParameters],
        log_level: Optional[str] = None,
        **kwargs: Any,
    ) -> AsyncGenerator[TOutput, None]:
        """
        Given a list of rows, it calls the process_row_fn for each row in parallel and yields the results

        :param rows: list of rows to process
        :param process_row_fn: function to process each row
        :param max_concurrent_tasks: maximum number of tasks to run concurrently
        :param parameters: parameters to pass to the process_row_fn
        :param log_level: log level
        :param kwargs: additional parameters
        :return: results of processing
        """
        semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrent_tasks)
        pending: Set[Task[TOutput]] = set()
        row_iterator: Iterator[TInput] = iter(rows)

        async def process_with_semaphore(*, row1: TInput) -> TOutput:
            async with semaphore:
                return await process_row_fn(
                    row=row1, parameters=parameters, log_level=log_level, **kwargs
                )

        # Initial filling of the pending set up to max_concurrent_tasks
        for _ in range(min(max_concurrent_tasks, len(rows))):
            row: TInput = next(row_iterator)
            pending.add(asyncio.create_task(process_with_semaphore(row1=row)))

        try:
            while pending:
                done: Set[Task[TOutput]]
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
