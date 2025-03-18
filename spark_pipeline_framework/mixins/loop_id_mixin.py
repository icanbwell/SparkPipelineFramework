from typing import Optional


class LoopIdMixin:
    def __init__(self) -> None:
        self.loop_id: Optional[str] = None

    def set_loop_id(self, loop_id: Optional[str]) -> None:
        """
        Set when running inside a FrameworkLoopTransformer

        :param loop_id: loop id
        """
        self.loop_id = loop_id
