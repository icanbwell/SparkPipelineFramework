class AttrDict(dict):
    """
    A wrapper around the dictionary that allows reference using dot notation e.g., dict.foo
    """

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self
