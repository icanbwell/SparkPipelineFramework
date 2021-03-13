class EventLogger:
    def log_progress_event(
        self,
        event_name: str,
        current: int,
        total: int,
        event_format_string: str,
        backoff: bool = True,
    ) -> None:
        """
        Logs a progress event
        :param event_name:
        :param current:
        :param total:
        :param event_format_string:
        :param backoff:
        """

    def log_event(self, event_name: str, event_text: str) -> None:
        """
        Logs a normal event

        :param event_name:
        :param event_text:
        """

    def log_exception(self, event_name: str, event_text: str, ex: Exception) -> None:
        """
        Logs an exception

        :param event_name:
        :param event_text:
        :param ex:
        """
