from glob import glob
from io import TextIOWrapper
from os import path, environ
from typing import Generator, List

from jsonlines.jsonlines import Reader
from insolvenzen.utils.storage import download_file

import jsonlines

from insolvenzen.utils.types import JSON

JSONL_PATH = "insolvenzbekanntmachungen-scraper"


def list_files() -> List[str]:
    if environ.get("USE_LOCAL_FILES"):
        base_dir = path.join(
            environ["LOCAL_FILES_TWENTYONE"],
            environ["BUCKET_SOURCE_NAME_TWENTYONE"],
            JSONL_PATH,
        )
        return sorted(glob(path.join(base_dir, "*.jsonl")))

    raise Exception(
        "This function only works locally, please define USE_LOCAL_FILES=1 and "
        "LOCAL_FILES_TWENTYONE=<bucket clone path> environment variables."
    )


def get_cases(filename: str) -> Generator[JSON, None, None]:

    if environ.get("USE_LOCAL_FILES"):
        reader = jsonlines.open(filename, "r")
    else:
        bio = download_file(filename, bucket=environ["BUCKET_SOURCE_NAME_TWENTYONE"])
        tio = TextIOWrapper(bio, encoding="utf-8")
        reader = Reader(tio)
        reader._should_close_fp = True

    with reader:
        yield from reader.iter()
