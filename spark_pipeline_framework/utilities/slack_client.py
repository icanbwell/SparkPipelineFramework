import json
from typing import Any

import requests
from requests.adapters import HTTPAdapter


class SlackClient:
    """
    Implements a basic Slack client in http to post messages to a channel
    From https://keestalkstech.com/2019/10/simple-python-code-to-send-message-to-slack-channel-without-packages/
    """

    def __init__(self, slack_token: str, channel: str, bot_user_name: str) -> None:
        self.slack_token = slack_token
        self.slack_channel = channel
        self.slack_icon_url = "https://www.flaticon.com/free-icon/freepik_23317"
        self.slack_user_name = bot_user_name

    def post_message_to_slack(self, text: str, blocks: Any = None) -> Any:
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
                }
            ),
            headers=headers,
        )
        return post.json()
