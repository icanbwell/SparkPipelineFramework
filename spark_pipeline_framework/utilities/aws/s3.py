import boto3
import re
from typing import Any, Dict, List, Optional, Tuple, Match, Pattern

# https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html gives a full list of restrictions for buckets
# https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html for keys
S3_URI_REGEX: Pattern[str] = re.compile(
    r"^s3a?://(?P<bucket>[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9])/(?P<key>[a-zA-Z0-9!\-_.*,'()/&$@=;:+ ,?]+)$"
)


def parse_s3_uri(s3_uri: str) -> Tuple[Optional[str], Optional[str]]:
    m: Optional[Match[str]] = S3_URI_REGEX.match(s3_uri)
    if m:
        return m.group("bucket"), m.group("key")
    else:
        return None, None


def get_s3_directory_contents(bucket: str, prefix: str) -> List[str]:
    """
    Returns all the S3 objects in the given bucket with the given prefix.

    Internally, just a light wrapper around list_objects_v2 that knows how to handle truncated responses.
    """
    client = boto3.client("s3")
    is_truncated: bool = True
    keys: List[str] = []
    kwargs: Dict[str, Optional[str]] = {
        "Bucket": bucket,
        "Prefix": prefix,
    }
    while is_truncated:
        response: Dict[str, Any] = client.list_objects_v2(**kwargs)
        keys.extend([k["Key"] for k in response["Contents"]])

        is_truncated = response["IsTruncated"]
        continuation_token: Optional[str] = response.get("NextContinuationToken")
        kwargs["ContinuationToken"] = continuation_token

    return keys
