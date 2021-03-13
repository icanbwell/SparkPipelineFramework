import pytest

from spark_pipeline_framework.utilities.slack_client import SlackClient


@pytest.mark.skip("Actually sends a message to slack")
def test_slack_client() -> None:
    slack_token = "put in token here to test"
    channel: str = "#helix_pipeline_notifications"
    bot_user_name = "Helix Pipeline Monitor"
    client = SlackClient(
        slack_token=slack_token, channel=channel, bot_user_name=bot_user_name
    )
    slack_info = f'There are *{19}* double images detected for *{22}* products. Please check the <https://{"foo"}.s3-eu-west-1.amazonaws.com/{"bar"}|Double Images Monitor>.'

    # blocks = [{
    #     "type": "section",
    #     "text": {
    #         "type": "mrkdwn",
    #         "text": ":check: The script has run successfully on the dev."
    #     }
    # }]

    response = client.post_message_to_slack(slack_info, blocks=None)
    print(response)
    response = client.post_message_to_slack("reply", blocks=None)
    print(response)
