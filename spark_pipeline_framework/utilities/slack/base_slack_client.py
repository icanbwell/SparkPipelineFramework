from typing import Any
from typing import Optional


class BaseSlackClient:
    def post_message_to_slack(
        self,
        text: str,
        blocks: Any = None,
        use_conversation_threads: bool = True,
        slack_thread: Optional[str] = None,
    ) -> Any:
        raise NotImplementedError
