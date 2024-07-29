import os
import sys
import traceback
from typing import Any, Optional, List

from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException

from spark_pipeline_framework.transformers.framework_mapping_runner.v1.framework_mapping_runner_exception import (
    FrameworkMappingRunnerException,
)
from spark_pipeline_framework.utilities.cannot_cast_exception_parser import (
    parse_cannot_cast_exception,
)


class FriendlySparkException(Exception):
    # noinspection PyUnusedLocal
    def __init__(
        self, exception: Exception, stage_name: Optional[str], *args: Any, **kwargs: Any
    ) -> None:
        """
        This exception wraps Spark Exceptions to extract out all the messages and show them

        :param exception:
        :param stage_name:
        :param args:
        :param kwargs:
        """
        try:
            self.exception: Exception = exception
            self.stage_name: Optional[str] = stage_name
            self.message: str = ""
            # Spark exceptions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.errors.html#classes
            if isinstance(exception, AnalysisException):
                self.message = str(exception)
            elif isinstance(exception, FrameworkMappingRunnerException):
                self.message = str(exception)
            elif isinstance(exception, Py4JJavaError):
                self.message = str(exception)
            else:
                # Summary is a boolean argument
                # If True, it prints the exception summary
                # This way, we can avoid printing the summary all
                # the way along the exception "bubbling up"
                error_text: str = (
                    stage_name
                    or ""
                    # + ": "
                    # + FriendlySparkException.exception_summary()
                )
                self.message = error_text

            # print(f"TEMPO exception type: {type(exception)}")
            exception_traceback = "".join(
                traceback.TracebackException.from_exception(exception).format()
            )
            self.message += "\n" + exception_traceback
            super().__init__(self.message)
        except KeyError:
            pass

    # noinspection SpellCheckingInspection
    @staticmethod
    def get_errortext(text: str) -> str:
        # Makes exception summary both BOLD and RED (FAIL)
        return text

    @staticmethod
    def exception_summary() -> str:
        # Gets the error stack
        msg = traceback.format_exc()
        try:
            # Builds the "frame" around the text
            # Gets the information about the error and makes it BOLD and RED
            info = list(
                filter(lambda t: len(t) and t[0] != "\t", msg.split("\n")[::-1])
            )
            error = FriendlySparkException.get_errortext("Error\t: {}".format(info[0]))
            # Figure out where the error happened - location (file/notebook), line and function
            idx = [t.strip()[:4] for t in info].index("File")
            where = [v.strip() for v in info[idx].strip().split(",")]
            location, line, func = where[0][5:], where[1][5:], where[2][3:]
            # If it is a pyspark error, just go with it
            if "pyspark" in error:
                new_msg = "\n{}".format(error)
            # Otherwise, build the summary
            else:
                new_msg = "\nLocation: {}\nLine\t: {}\nFunction: {}\n{}".format(
                    location, line, func, error
                )
            # now loop  through __cause__ to build up message including all the error messages
            type_, exc, tr = sys.exc_info()
            if exc:
                current_exception: BaseException = exc
                i: int = 0
                while (
                    hasattr(current_exception, "__cause__")
                    and current_exception.__cause__
                ):
                    current_exception = current_exception.__cause__
                    file_name: str = ""
                    line_number: Optional[int] = None
                    if (
                        hasattr(current_exception, "__traceback__")
                        and current_exception.__traceback__
                    ):
                        file_name = os.path.basename(
                            current_exception.__traceback__.tb_frame.f_code.co_filename
                        )
                        line_number = (
                            current_exception.__traceback__.tb_frame.f_code.co_firstlineno
                        )
                    i += 1
                    exception_message: str = str(current_exception)
                    if "cannot cast " in exception_message:
                        result: List[str] = parse_cannot_cast_exception(
                            exception_message
                        )
                        for item in result:
                            new_msg += (
                                "\n"
                                + ("<" * i)
                                + " "
                                + item
                                + " ["
                                + file_name
                                + (f" ({line_number})" if line_number else "")
                                + "]"
                            )
                    new_msg += (
                        "\n"
                        + ("<" * i)
                        + " "
                        + exception_message
                        + " ["
                        + file_name
                        + (f" ({line_number})" if line_number else "")
                        + "]"
                    )

            return new_msg
        except Exception as e:
            # If we managed to raise an exception while trying to format the original exception...
            # Oh, well...
            return "This is awkward... \n{}".format(str(e))
