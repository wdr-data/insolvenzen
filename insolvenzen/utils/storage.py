import os
from io import BytesIO
from datetime import datetime
from typing import Callable, Literal, Optional, Union
import posixpath

import pytz
import pandas as pd
from loguru import logger

from botocore.exceptions import ClientError
from boto3 import client
import sentry_sdk

BUCKET_PUBLIC = None
try:
    s3 = client("s3")
    BUCKET_PUBLIC = os.environ["BUCKET_PUBLIC_NAME"]
except Exception as e:
    logger.info("Warning: s3 client not created: {}", e)


def simple_compare(old, new):
    return old == new


def make_df_compare_fn(*, ignore_columns=None):
    def is_equal(old, new):
        old = pd.read_csv(BytesIO(old))
        new = pd.read_csv(BytesIO(new))

        if ignore_columns is not None:
            old = old.drop(columns=ignore_columns, errors="ignore")
            new = new.drop(columns=ignore_columns, errors="ignore")

        return old.equals(new)

    return is_equal


def download_file(filename, *, bucket=BUCKET_PUBLIC):
    bio = BytesIO()
    s3.download_fileobj(bucket, filename, bio)
    bio.seek(0)
    return bio


def upload_dataframe(
    df: pd.DataFrame,
    filename: str,
    *,
    index: bool = True,
    change_notification: Optional[str] = None,
    compare: Optional[Union[Callable, Literal[False]]] = None,
    bucket: str = BUCKET_PUBLIC,
    archive: bool = True,
    public_read: True,
):
    logger.info("Uploading dataframe...")
    if compare is None:
        compare = simple_compare

    # Convert to csv and encode to get bytes
    write = df.to_csv(
        index=index,
        line_terminator="\n",
        encoding="utf-8",
    ).encode("utf-8")

    # Read old file-like object to check for differences
    compare_failed = False

    if compare is not False:
        logger.debug("Comparing...")
        bio_old = BytesIO()

        try:
            s3.download_fileobj(bucket, filename, bio_old)
        except ClientError:
            logger.warning("Compare failed!")
            compare_failed = True

        bio_old.seek(0)
        if not compare_failed and compare(bio_old.read(), write):
            logger.success("No need to update file, done.")
            return

    # Notify
    if change_notification and not compare_failed:
        logger.debug("Send notification via Sentry...")
        sentry_sdk.capture_message(change_notification)

    # Create new file-like object for upload
    bio_new = BytesIO(write)

    # Upload file with ACL and content type
    logger.debug("Uploading file...")
    s3.upload_fileobj(
        bio_new,
        bucket,
        filename,
        ExtraArgs={
            "ACL": "public-read" if public_read else "private",
            "ContentType": "text/plain; charset=utf-8",
        },
    )

    if not archive:
        return

    # Upload file again into timestamped folder
    logger.debug("Uploading archive version of file...")
    bio_new = BytesIO(write)
    now = datetime.now(tz=pytz.timezone("Europe/Berlin"))
    timestamp = now.date().isoformat()
    *path, filename = posixpath.split(filename)
    s3.upload_fileobj(
        bio_new,
        bucket,
        f"{posixpath.join(*path)}/{timestamp}/{filename}".lstrip("/"),
        ExtraArgs={
            "ACL": "public-read" if public_read else "private",
            "ContentType": "text/plain; charset=utf-8",
        },
    )
