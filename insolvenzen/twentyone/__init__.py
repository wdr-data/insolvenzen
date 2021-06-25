from typing import Any

from insolvenzen.twentyone.exporter import export_cases
from insolvenzen.twentyone.extractor import extract_features
from insolvenzen.twentyone.loader import get_cases, list_files


def run():
    for filename in list_files():
        handle_file(filename)


def run_concurrent():
    from concurrent.futures import ProcessPoolExecutor

    executor = ProcessPoolExecutor()
    futures = []
    for filename in list_files():
        future = executor.submit(handle_file, filename)
        futures.append(future)

    for future in futures:
        future.result()


def handle_file(filename: str):
    cases = []
    for case in get_cases(filename):
        cases.append(extract_features(case))

    export_cases(cases, filename)


def new_file_handler(event: Any, context: Any):
    ...
