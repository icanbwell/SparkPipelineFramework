import asyncio
from asyncio import Task
from dataclasses import dataclass
from typing import AsyncGenerator, Protocol, List, Optional, Set, Dict, Any


@dataclass
class ParallelFunctionContext:
    """
    This class contains the parameters for a parallel function
    """

    """ name of the processor """
    name: str

    """ log level """
    log_level: Optional[str]

    """ index of the task """
    task_index: int

    total_task_count: int


class ParallelFunction[TInput, TOutput, TParameters](Protocol):
    async def __call__(
        self,
        *,
        context: ParallelFunctionContext,
        row: TInput,
        parameters: Optional[TParameters],
        **kwargs: Any,
    ) -> TOutput:
        """
        Handle a batch of data

        :param name: name of the processor
        :param row: row to process
        :param parameters: parameters to pass to the process_row_fn
        :param log_level: log level
        :param task_index: index of the task
        :param total_task_count: total number of tasks
        :param kwargs: additional parameters
        :return: result of processing
        """
        ...


class AsyncParallelProcessor:
    def __init__(self, *, name: str, max_concurrent_tasks: int) -> None:
        """
        This class is used to process rows in parallel

        :param name: name of the processor
        :param max_concurrent_tasks: maximum number of concurrent tasks
        """
        self.name: str = name
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

        # noinspection PyShadowingNames
        async def process_with_semaphore(
            *, name: str, row1: TInput, task_index: int, total_task_count: int
        ) -> TOutput:
            async with self.semaphore:
                return await process_row_fn(
                    name=name,
                    row=row1,
                    parameters=parameters,
                    log_level=log_level,
                    task_index=task_index,
                    total_task_count=total_task_count,
                    **kwargs,
                )

        total_task_count: int = len(rows)
        # Create all tasks at once with their indices
        pending: Set[Task[TOutput]] = {
            asyncio.create_task(
                process_with_semaphore(
                    name=self.name,
                    row1=row,
                    task_index=i,
                    total_task_count=total_task_count,
                ),
                name=f"task_{i}",  # Optionally set task name for easier debugging
            )
            for i, row in enumerate(rows)
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
