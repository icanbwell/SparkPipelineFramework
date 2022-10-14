class FileWriteModes:
    MODE_APPEND = "append"
    MODE_OVERWRITE = "overwrite"
    MODE_IGNORE = "ignore"
    MODE_ERROR = "error"
    MODE_CHOICES = (
        MODE_APPEND,
        MODE_OVERWRITE,
        MODE_IGNORE,
        MODE_ERROR,
    )


class FileReadModes:
    MODE_PERMISSIVE = "permissive"
    MODE_DROP_MALFORMED = "dropMalformed"
    MODE_FAIL_FAST = "failFast"
    MODE_CHOICES = (
        MODE_PERMISSIVE,
        MODE_DROP_MALFORMED,
        MODE_FAIL_FAST,
    )


class FileJsonTypes:
    NDJSON = "ndjson"
    JSON = "json"
