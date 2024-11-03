import asyncio
from asyncio import Task
from typing import AsyncGenerator, Protocol, List, Optional, Set, Dict, Any


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
    def __init__(self, *, max_concurrent_tasks: int) -> None:
        self.max_concurrent_tasks: int = max_concurrent_tasks
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def process_rows_in_parallel[
        TInput, TOutput, TParameters: Dict[str, Any] | object
    ](
        self,
        rows: List[TInput],
        process_row_fn: ParallelFunction[TInput, TOutput, TParameters],
        parameters: Optional[TParameters],
        log_level: Optional[str] = None,
        **kwargs: Any,
    ) -> AsyncGenerator[TOutput, None]:
        """
        Given a list of rows, it calls the process_row_fn for each row in parallel and yields the results

        :param rows: list of rows to process
        :param process_row_fn: function to process each row
        :param parameters: parameters to pass to the process_row_fn
        :param log_level: log level
        :param kwargs: additional parameters
        :return: results of processing
        """

        async def process_with_semaphore(*, row1: TInput) -> TOutput:
            async with self.semaphore:
                return await process_row_fn(
                    row=row1, parameters=parameters, log_level=log_level, **kwargs
                )

        # Create all tasks at once
        pending: Set[Task[TOutput]] = {
            asyncio.create_task(process_with_semaphore(row1=row)) for row in rows
        }

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

        finally:
            # Cancel any pending tasks if something goes wrong
            for task in pending:
                task.cancel()
