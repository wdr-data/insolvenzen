from loguru import logger
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
    logger.info("Handling file {}", filename)
    cases = []
    for case in get_cases(filename):
        cases.append(extract_features(case))

    export_cases(cases, filename)


def new_file_handler(event: dict, context: dict):
    logger.info("S3 Trigger activated")
    print(event)
    for record in event["Records"]:
        object_key = record["s3"]["object"]["key"]
        handle_file(object_key)
