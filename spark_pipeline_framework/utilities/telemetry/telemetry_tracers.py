from enum import Enum


class TelemetryTracer(Enum):
    ASYNCIO = "asyncio"
    AIOHTTP = "aiohttp"
    BOTO = "boto"
    BOTOCORE = "botocore"
    CONFLUENT_KAFKA = "confluent_kafka"
    ELASTICSEARCH = "elasticsearch"
    HTTPX = "httpx"
    PYMONGO = "pymongo"
    REQUESTS = "requests"
    SYSTEM = "system"
