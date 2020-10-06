def get_first_class_in_module(module):
    """
    Gets the first class in the specified module
    :param module:
    :return:
    """
    md = module.__dict__
    return [md[c] for c in md if (isinstance(md[c], type) and md[c].__module__ == module.__name__)][0]
