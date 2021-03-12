class EventLogger:
    def log_progress_event(
        self, event_name: str, current: int, total: int, event_format_string: str
    ) -> None:
        pass

    def log_event(self, event_name: str, event_text: str) -> None:
        pass
