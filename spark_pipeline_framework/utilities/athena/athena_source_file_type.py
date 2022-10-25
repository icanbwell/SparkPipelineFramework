from enum import Enum


class AthenaSourceFileType(Enum):
    CSV_QUOTED = 1
    CSV_UNQUOTED = 2
    JSONL = 3
    PARQUET = 4
