from typing import Any, List


def diff_lists(li1: List[Any], li2: List[Any]) -> List[Any]:
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif
