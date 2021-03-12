from spark_pipeline_framework.event_loggers.event_logger import EventLogger

from spark_pipeline_framework.utilities.slack_client import SlackClient


class SlackEventLogger(EventLogger):
    def __init__(
        self, slack_token: str, slack_channel: str, bot_user_name: str
    ) -> None:
        super().__init__()
        assert slack_token
        assert slack_channel
        assert bot_user_name
        self.slack_token: str = slack_token
        self.slack_channel: str = slack_channel
        self.slack_user_name: str = bot_user_name
        self.slack_client: SlackClient = SlackClient(
            slack_token=slack_token, channel=slack_channel, bot_user_name=bot_user_name
        )

    def log_progress_event(
        self, event_name: str, current: int, total: int, event_format_string: str
    ) -> None:
        self.slack_client.post_message_to_slack(
            event_format_string.format(event_name, current, total)
        )

    def log_event(self, event_name: str, event_text: str) -> None:
        self.slack_client.post_message_to_slack(event_text)

    def log_exception(self, event_name: str, event_text: str, ex: Exception) -> None:
        # don't send full exception to slack since it can have PHI
        self.slack_client.post_message_to_slack(
            f"{event_name}: {event_text} {type(ex)}"
        )
