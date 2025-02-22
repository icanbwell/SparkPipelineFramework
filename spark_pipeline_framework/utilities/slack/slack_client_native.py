import json
from datetime import datetime, timedelta
from logging import getLogger
from typing import Any, Optional

# noinspection PyUnresolvedReferences
from slack_sdk.web.client import WebClient

# noinspection PyUnresolvedReferences
from slack_sdk.web.slack_response import SlackResponse

# noinspection PyUnresolvedReferences
from slack_sdk.errors import SlackApiError

from spark_pipeline_framework.utilities.slack.base_slack_client import BaseSlackClient


logger = getLogger(__name__)


class SlackClientNative(BaseSlackClient):
    """
    Implements a native Slack client
    """

    def __init__(self, slack_token: str, channel: str, bot_user_name: str) -> None:
        """
        Client for sending messages to Slack

        :param slack_token: token to auth with Slack
        :param channel: channel to post message into
        :param bot_user_name: user name to use when posting messages
        """
        self.slack_token: str = slack_token
        self.slack_channel: str = channel
        self.slack_icon_url: str = "https://www.flaticon.com/free-icon/freepik_23317"
        self.slack_user_name: str = bot_user_name
        self.slack_thread: Optional[str] = None
        # if we have to wait till a time before slack will let us send messages
        self.wait_till_datetime: Optional[datetime] = None

    def post_message_to_slack(
        self,
        text: str,
        blocks: Any = None,
        use_conversation_threads: bool = True,
        slack_thread: Optional[str] = None,
    ) -> Any:
        """
        Posts a message to Slack
        :param text: message to post.  Can be markdown.
        :param blocks: (Optional) blocks to post
        :param use_conversation_threads: whether to send messages as reply to first message
        :return: response from Slack
        """
        if not text:
            return None

        if (
            self.wait_till_datetime is not None
            and self.wait_till_datetime > datetime.utcnow()
        ):
            return None

        try:
            web = WebClient(token=self.slack_token)
            response: SlackResponse = web.chat_postMessage(
                text=text,
                channel=self.slack_channel,
                thread_ts=(
                    slack_thread if slack_thread is not None else self.slack_thread
                ),
                icon_url=self.slack_icon_url,
                username=self.slack_user_name,
                blocks=json.dumps(blocks) if blocks else None,
            )

            if not self.slack_thread and use_conversation_threads:
                if (
                    response.status_code == 200
                    and response.data
                    and "ts" in response.data
                ):
                    self.slack_thread = response.data["ts"]  # type: ignore
            self.wait_till_datetime = None  # clear this on success
            return response
        except SlackApiError as e:
            logger.warning(f"Slack API Error: {e.response['error']}")
            if e.response.status_code == 429:
                # The `Retry-After` header will tell you how long to wait before retrying
                delay_in_seconds: int = int(e.response.headers["Retry-After"])
                logger.warning(f"Rate limited. Retrying in {delay_in_seconds} seconds")
                self.wait_till_datetime = datetime.utcnow() + timedelta(
                    seconds=delay_in_seconds
                )
            return None
        except Exception as e:
            logger.warning(f"Unknown error calling Slack API: {e}")
            return None
