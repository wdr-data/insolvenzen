import json
from io import BytesIO
import os
from os import path
from enum import Enum

from boto3 import client
from loguru import logger


class InsolvencyType(Enum):
    PRIVATE = "ik"
    REGULAR = "in"


class CaseType(Enum):
    VERFAHRENEROEFFNET = "verfahreneroeffnet"
    ABWEISUNGMANGELSMASSE = "abweisungmangelsmasse"
    SICHERUNGSMASSNAHMEN = "sicherungsmassnahmen"


try:
    s3 = client("s3")
    bucket = os.environ["BUCKET_SOURCE_NAME"]
except Exception as e:
    logger.info("Warning: s3 client for source not created: {}", e)

USE_LOCAL_FILES = bool(os.environ.get("USE_LOCAL_FILES"))


def load_source_file(insolvency_type: InsolvencyType, filename: str):
    if USE_LOCAL_FILES:
        with open(
            path.join(os.environ["LOCAL_FILES"], insolvency_type.value, filename)
        ) as fp:
            data = json.load(fp)

        return data

    filename = f"{insolvency_type.value}/{filename}"
    bio = BytesIO()
    s3.download_fileobj(bucket, filename, bio)
    bio.seek(0)
    return json.load(bio)


def list_files(insolvency_type: InsolvencyType):
    if USE_LOCAL_FILES:
        return sorted(
            os.listdir(path.join(os.environ["LOCAL_FILES"], insolvency_type.value))
        )

    prefix = insolvency_type.value + "/"
    return [
        key.replace(prefix, "")
        for key in get_matching_s3_keys(prefix=prefix, suffix=".json")
    ]


# Source: https://alexwlchan.net/2019/07/listing-s3-keys/
def get_matching_s3_objects(prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {"Bucket": bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix,)
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(prefix, suffix):
        yield obj["Key"]
