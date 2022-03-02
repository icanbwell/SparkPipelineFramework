from typing import Optional

from spark_pipeline_framework.event_loggers.event_logger import EventLogger
from spark_pipeline_framework.utilities.slack.base_slack_client import BaseSlackClient

from spark_pipeline_framework.utilities.slack.slack_client import SlackClient
from spark_pipeline_framework.utilities.slack.slack_client_native import (
    SlackClientNative,
)


class SlackEventLogger(EventLogger):
    def __init__(
        self,
        id_: str,
        slack_token: str,
        slack_channel: str,
        bot_user_name: str,
        slack_error_channel: Optional[str] = None,
        log_placeholder_url: Optional[str] = None,
        flow_run_name: Optional[str] = None,
        backoff: bool = True,
        use_native_slack_client: bool = False,
    ) -> None:
        """
        Logs events to Slack

        :param id_: unique id for this logger
        :param slack_token: token to auth with Slack
        :param slack_channel: channel to post message into
        :param slack_error_channel: channel to post error message into
        :param bot_user_name: user name to use when posting messages
        :param log_placeholder_url: url to logs with a placeholder for flow_run_name to be replaced by id
                e.g., https://grafana.services.icanbwell.com/explore?orgId=1&left=%5B%22now-1h%22,%22
                        now%22,%22loki.staging.icanbwell.com%22,%7B%22expr%22:%22%7Bflow_run_name%3D%5C%22
                        {flow_run_name}%5C%22%7D%22%7D%5D
        :param backoff: whether to backoff sending too many messages to slack
        :param flow_run_name: flow run name to set in log_placeholder_url
        :param use_native_slack_client: whether to use the native slack client or use plain HTTP post
        """
        super().__init__()
        assert slack_token
        assert slack_channel
        assert bot_user_name
        self.id_: str = id_
        self.slack_token: str = slack_token
        self.slack_channel: str = slack_channel
        self.slack_error_channel: Optional[str] = slack_error_channel
        self.slack_user_name: str = bot_user_name
        self.backoff: bool = backoff
        self.slack_client: BaseSlackClient = (
            SlackClientNative(
                slack_token=slack_token,
                channel=slack_channel,
                bot_user_name=bot_user_name,
            )
            if use_native_slack_client
            else SlackClient(
                slack_token=slack_token,
                channel=slack_channel,
                bot_user_name=bot_user_name,
            )
        )
        self.slack_error_client: Optional[BaseSlackClient] = (
            (
                SlackClientNative(
                    slack_token=slack_token,
                    channel=slack_error_channel,
                    bot_user_name=bot_user_name,
                )
                if use_native_slack_client
                else SlackClient(
                    slack_token=slack_token,
                    channel=slack_error_channel,
                    bot_user_name=bot_user_name,
                )
            )
            if slack_error_channel
            else None
        )
        self.flow_run_name: Optional[str] = flow_run_name
        self.log_placeholder_url: Optional[str] = log_placeholder_url
        self.has_sent_log_url: bool = False

    def log_progress_event(
        self,
        event_name: str,
        current: int,
        total: int,
        event_format_string: str,
        backoff: bool = True,
    ) -> None:
        # for first 10 batches send every time
        # for 10 to 100 send every 10 batches
        # after 100 send every 100 batches
        if (
            not self.backoff
            or not backoff
            or (current < 10)
            or (current < 100 and current % 10 == 0)
            or (current < 1000 and current % 100 == 0)
            or (current < 10000 and current % 1000 == 0)
            or (current < 100000 and current % 10000 == 0)
            or (current < 1000000 and current % 100000 == 0)
        ):
            self.slack_client.post_message_to_slack(
                event_format_string.format(event_name, current, total)
            )

    def log_event(self, event_name: str, event_text: str) -> None:
        self.slack_client.post_message_to_slack(event_text)
        if not self.has_sent_log_url:
            if self.log_placeholder_url:
                log_url = self.get_grafana_url()
                if log_url:
                    self.slack_client.post_message_to_slack(log_url)
            self.has_sent_log_url = True

    def log_exception(self, event_name: str, event_text: str, ex: Exception) -> None:
        # don't send full exception to slack since it can have PHI
        self.slack_client.post_message_to_slack(
            f"Helix Pipeline Failure: {self.id_} {event_name}: {event_text} {type(ex)}. <{self.get_grafana_url()}>"
        )

        if self.slack_error_client:
            self.slack_error_client.post_message_to_slack(
                f"Helix Pipeline Failure: {self.id_} {event_name}: {event_text} {type(ex)}. <{self.get_grafana_url()}>"
            )

    def get_grafana_url(self) -> Optional[str]:
        return (
            self.log_placeholder_url.format(flow_run_name=f"{self.flow_run_name}")
            if self.log_placeholder_url
            else None
        )
