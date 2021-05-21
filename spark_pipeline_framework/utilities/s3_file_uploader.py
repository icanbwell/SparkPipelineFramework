import boto3


def getS3Resource():
    return boto3.resource("s3")


def check_bucket_exists(bucket_name: str):
    s3_resource = getS3Resource()
    if s3_resource.Bucket(bucket_name).creation_date is not None:
        return True
    return False


def upload_file_to_S3(local_file_path: str, s3_file_path: str, bucket_name: str):
    s3_resource = getS3Resource()
    s3_resource.Bucket(bucket_name).upload_file(
        local_file_path, s3_file_path,
    )
