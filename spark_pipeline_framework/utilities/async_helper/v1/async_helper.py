from typing import AsyncGenerator, List, TypeVar

T = TypeVar("T")


class AsyncHelper:
    @staticmethod
    async def collect_items(generator: AsyncGenerator[T, None]) -> List[T]:
        items = []
        async for item in generator:
            items.append(item)
        return items
