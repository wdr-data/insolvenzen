from collections import defaultdict
import datetime as dt
from functools import lru_cache

from loguru import logger

from insolvenzen.utils.source import (
    CaseType,
    load_source_file,
    list_files,
)


def signed(number):
    prefix = ""
    if number >= 0:
        prefix = "+"

    return f"{prefix}{number}"


def in_nrw(residence):
    """
    Checks if the given residence is in NRW
    """
    return (
        residence["geolocation-street"]["street-gemeinde"]["gemeinde-kreis"][
            "kreis-bundesland"
        ]["bundesland-name"]
        == "Nordrhein-Westfalen"
    )


@lru_cache
def get_files(insolvency_type):
    filenames = list_files(insolvency_type)
    files = {
        dt.date.fromisoformat(filename[:10]): load_source_file(
            insolvency_type, filename
        )
        for filename in filenames
    }
    return files


@lru_cache
def filter_data(insolvency_type):
    files = get_files(insolvency_type)

    court_case_numbers = {case_type: set() for case_type in CaseType}
    cases = {case_type: list() for case_type in CaseType}
    stats = {case_type: defaultdict(int) for case_type in CaseType}

    def process_cases(date, fil, case_type):
        for case in fil.get(case_type.value, []):
            # Count total cases
            stats[case_type]["total_cases"] += 1

            # Test if entry contains courtcase residences
            if not case.get("courtcase-residences", []):
                stats[case_type]["no_courtcase_residences"] += 1
                continue

            # Not in NRW
            if not any(
                in_nrw(residence) for residence in case.get("courtcase-residences", [])
            ):
                continue

            court_case_number = case["courtcase-aktenzeichen"]
            court = case["courtcase-court"]
            unique_case_number = (court, court_case_number)

            # Duplicate court case number
            if unique_case_number in court_case_numbers[case_type]:
                stats[case_type]["nrw_duplicates"] += 1
                continue

            court_case_numbers[case_type].add(unique_case_number)

            # Add custom properties
            case["date"] = date

            # Add proceeding to the list
            cases[case_type].append(case)

    # Filter irrelevant and duplicate values
    for date, fil in files.items():
        process_cases(date, fil, CaseType.ABWEISUNGMANGELSMASSE)
        process_cases(date, fil, CaseType.SICHERUNGSMASSNAHMEN)
        process_cases(date, fil, CaseType.VERFAHRENEROEFFNET)

    logger.info(
        f"Found a total of {stats[CaseType.VERFAHRENEROEFFNET]['total_cases']} in all of DE"
    )
    logger.info(
        f"No courtcase-residences found for "
        f"{stats[CaseType.VERFAHRENEROEFFNET]['no_courtcase_residences']} out of those proceedings "
        f"({round(stats[CaseType.VERFAHRENEROEFFNET]['no_courtcase_residences'] / stats[CaseType.VERFAHRENEROEFFNET]['total_cases'], 1)}%)"
    )

    logger.info(
        "Found {} relevant proceedings",
        len(cases[CaseType.VERFAHRENEROEFFNET]),
    )
    logger.info(
        f"{stats[CaseType.VERFAHRENEROEFFNET]['nrw_duplicates']} proceedings in "
        "NRW were discarded due to referring to the same court + case number"
    )

    return cases, stats


def clear_caches():
    get_files.cache_clear()
    filter_data.cache_clear()
