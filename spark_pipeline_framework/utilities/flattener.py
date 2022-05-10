from typing import List, Any


def flatten(my_list: List[Any]) -> List[Any]:
    """

    :param my_list:
    :return:
    """
    if not my_list:
        return my_list
    flat_list: List[Any] = []
    for element in my_list:
        if isinstance(element, list):
            flat_list.extend(flatten(element))
        else:
            flat_list.append(element)
    return flat_list


# def create_stages(transformer_list: List, parameters, progress_logger):
#     return [t(parameters, progress_logger) for t in flatten(transformer_list)]
