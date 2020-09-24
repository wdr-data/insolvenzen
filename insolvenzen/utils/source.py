import json
from io import BytesIO
import os
from enum import Enum

from boto3 import client


class InsolvencyType(Enum):
    PRIVATE = "ik"
    REGULAR = "in"


try:
    s3 = client("s3")
    bucket = os.environ["BUCKET_SOURCE_NAME"]
except Exception as e:
    print("Warning: s3 client for source not created:", e)


def load_source_file(insolvency_type: InsolvencyType, filename: str):
    from os import path

    with open(
        path.join(os.environ["LOCAL_FILES"], insolvency_type.value, filename)
    ) as fp:
        data = json.load(fp)

    return data

    # Live version - not working yet
    bio = BytesIO()
    s3.download_fileobj(bucket, filename, bio)
    bio.seek(0)
    return json.load(bio)


def list_files(insolvency_type: InsolvencyType):
    from os import path

    return sorted(
        os.listdir(path.join(os.environ["LOCAL_FILES"], insolvency_type.value))
    )
