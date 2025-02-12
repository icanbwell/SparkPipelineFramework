from typing import Any


class BaseSlackClient:
    def post_message_to_slack(
        self, text: str, blocks: Any = None, use_conversation_threads: bool = True , slack_thread: str =None,
    ) -> Any:
        raise NotImplementedError
