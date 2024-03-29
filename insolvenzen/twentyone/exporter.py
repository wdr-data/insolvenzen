from loguru import logger
from insolvenzen.utils.storage import upload_dataframe
from typing import Dict, List
from os import path, environ, makedirs
import posixpath

import pandas as pd

BUCKET_EXTRACT_NAME = environ.get("BUCKET_EXTRACT_NAME")


def export_cases(cases: List[Dict], original_filename: str):
    logger.info("Exporting {} cases for {}", len(cases), original_filename)

    if not cases:
        logger.warning("No cases, skipping")
        return

    df = pd.DataFrame.from_records(data=cases)

    if environ.get("USE_LOCAL_FILES"):
        export_filename = path.join("extracted", original_filename + ".csv")
        export_path = path.join(*path.split(export_filename)[:-1])

        makedirs(export_path, exist_ok=True)
        with open(export_filename, "w", encoding="utf-8") as fp:
            df.to_csv(fp, index=False, line_terminator="\n")

    else:
        export_filename = original_filename + ".csv"
        upload_dataframe(
            df,
            export_filename,
            bucket=BUCKET_EXTRACT_NAME,
            index=False,
            archive=False,
            public_read=False,
        )

    logger.success("Exporting {} finished!", export_filename)
