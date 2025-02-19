import json
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter

from spark_pipeline_framework.utilities.slack.base_slack_client import BaseSlackClient


class SlackClient(BaseSlackClient):
    """
    Implements a basic Slack client in http to post messages to a channel
    Inspired by https://keestalkstech.com/2019/10/simple-python-code-to-send-message-to-slack-channel-without-packages/
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
            return

        headers = {
            "Content-type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {self.slack_token}",
        }
        adapter = HTTPAdapter()
        http = requests.Session()
        http.mount("https://", adapter)

        post = http.post(
            "https://slack.com/api/chat.postMessage",
            data=json.dumps(
                {
                    "channel": self.slack_channel,
                    "text": text,
                    "icon_url": self.slack_icon_url,
                    "username": self.slack_user_name,
                    "blocks": json.dumps(blocks) if blocks else None,
                    "thread_ts": (
                        slack_thread if slack_thread is not None else self.slack_thread
                    ),
                }
            ),
            headers=headers,
        )
        response_json = post.json()
        if not self.slack_thread and use_conversation_threads:
            if "ok" in response_json and response_json["ok"] is True:
                # read thread id from response
                if "message" in response_json and "ts" in response_json["message"]:
                    self.slack_thread = response_json["message"]["ts"]
        return response_json
