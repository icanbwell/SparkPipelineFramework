from typing import List


def flatten(my_list: List) -> List:
    """

    :param my_list:
    :return:
    """
    if not my_list:
        return my_list
    if isinstance(my_list[0], list):
        return flatten(my_list[0]) + flatten(my_list[1:])
    return my_list[:1] + flatten(my_list[1:])
