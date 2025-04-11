import os
import traceback
from typing import Any, Optional, List, Dict

from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException

from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner_exception import (
    FrameworkMappingRunnerException,
)
from spark_pipeline_framework.utilities.cannot_cast_exception_parser import (
    parse_cannot_cast_exception,
)


class FriendlySparkException(Exception):
    """
    A comprehensive exception handler for Spark-related errors.

    Provides detailed error context, traceback, and nested exception information.
    """

    def __init__(
        self,
        exception: Exception,
        stage_name: Optional[str] = None,
        message: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Initialize a friendly Spark exception with comprehensive error details.

        :param exception: The original exception
        :param stage_name: Name of the stage where the exception occurred
        :param message: Additional error message
        :param args: Additional positional arguments
        :param kwargs: Additional keyword arguments
        """
        # Initialize base error attributes
        self.original_exception: Exception = exception
        self.stage_name: Optional[str] = stage_name

        # Construct detailed error message
        error_message = self._construct_error_message(exception, stage_name, message)

        # Call parent exception constructor
        super().__init__(error_message)

    def _construct_error_message(
        self, exception: Exception, stage_name: Optional[str], message: Optional[str]
    ) -> str:
        """
        Construct a comprehensive error message.

        :param exception: The original exception
        :param stage_name: Name of the stage where the exception occurred
        :param message: Additional error message
        :return: Formatted error message
        """
        # Start with optional custom message
        full_message = f"{message}\n" if message else ""

        # Handle specific exception types
        exception_type = type(exception)
        exception_handlers = {
            AnalysisException: str,
            FrameworkMappingRunnerException: str,
            Py4JJavaError: str,
            Exception: str,
        }

        # Use specific handler or default to str()
        handler = exception_handlers.get(exception_type, str)
        full_message += handler(exception)

        # Add stage name if available
        if stage_name:
            full_message += f"\nStage: {stage_name}"

        # Add full traceback
        full_message += "\n" + self._get_exception_traceback(exception)

        return full_message

    def _get_exception_traceback(self, exception: Exception) -> str:
        """
        Generate a formatted traceback for the given exception.

        :param exception: The exception to generate traceback for
        :return: Formatted traceback string
        """
        return "".join(traceback.TracebackException.from_exception(exception).format())

    @classmethod
    def parse_nested_exceptions(cls, exception: Exception) -> List[str]:
        """
        Parse nested exceptions to extract detailed error information.

        :param exception: The root exception
        :return: List of parsed exception messages
        """
        nested_messages = []
        current_exception: Optional[BaseException] = exception

        while current_exception:
            # Extract exception details
            file_name = cls._get_exception_file_name(current_exception)
            line_number = cls._get_exception_line_number(current_exception)

            # Handle specific parsing for cast exceptions
            exception_message = str(current_exception)
            if "cannot cast " in exception_message:
                cast_parse_result = parse_cannot_cast_exception(exception_message)
                nested_messages.extend(
                    [
                        f"{item} [{file_name} {'(' + str(line_number) + ')' if line_number else ''}]"
                        for item in cast_parse_result
                    ]
                )

            # Add general exception message
            nested_messages.append(
                f"{exception_message} [{file_name} {'(' + str(line_number) + ')' if line_number else ''}]"
            )

            # Move to next nested exception
            current_exception = getattr(current_exception, "__cause__", None)

        return nested_messages

    @staticmethod
    def _get_exception_file_name(exception: BaseException) -> str:
        """
        Get the filename where the exception occurred.

        :param exception: The exception
        :return: Filename or empty string
        """
        if hasattr(exception, "__traceback__") and exception.__traceback__:
            return os.path.basename(exception.__traceback__.tb_frame.f_code.co_filename)
        return ""

    @staticmethod
    def _get_exception_line_number(exception: BaseException) -> Optional[int]:
        """
        Get the line number where the exception occurred.

        :param exception: The exception
        :return: Line number or None
        """
        if hasattr(exception, "__traceback__") and exception.__traceback__:
            return exception.__traceback__.tb_frame.f_code.co_firstlineno
        return None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception details to a dictionary for logging.

        :return: Dictionary of exception details
        """
        return {
            "message": str(self),
            "stage_name": self.stage_name,
            "exception_type": type(self.original_exception).__name__,
            "nested_exceptions": self.parse_nested_exceptions(self.original_exception),
        }
