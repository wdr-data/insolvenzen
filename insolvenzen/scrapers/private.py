import re
from datetime import datetime, time
from functools import lru_cache
import datetime as dt
from collections import defaultdict

import requests
import pandas as pd
import pytz
import sentry_sdk

from insolvenzen.utils.storage import upload_dataframe
from insolvenzen.utils.source import InsolvencyType, load_source_file, list_files


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
def get_data():
    # Download website
    filenames = list_files(InsolvencyType.PRIVATE)
    files = {
        dt.date.fromisoformat(filename[:10]): load_source_file(
            InsolvencyType.PRIVATE, filename
        )
        for filename in filenames
    }
    return files


def clear_data():
    files = get_data()

    proceedings = []
    court_case_numbers = set()

    # Statistics overall
    total_proceedings = 0
    no_courtcase_residences = 0

    # Statistics in NRW
    nrw_duplicates = 0

    # Filter irrelevant and duplicate values
    for date, fil in files.items():
        # No proceedings on this day
        if "verfahreneroeffnet" not in fil:
            continue

        for proceeding in fil["verfahreneroeffnet"]:
            # Count total proceedings
            total_proceedings += 1

            # Test if entry contains courtcase residences
            if not proceeding.get("courtcase-residences", []):
                no_courtcase_residences += 1
                continue

            # Not in NRW
            if not any(
                in_nrw(residence)
                for residence in proceeding.get("courtcase-residences", [])
            ):
                continue

            court_case_number = proceeding["courtcase-aktenzeichen"]
            court = proceeding["courtcase-court"]
            unique_case_number = (court, court_case_number)

            # Duplicate court case number
            if unique_case_number in court_case_numbers:
                nrw_duplicates += 1
                continue

            court_case_numbers.add(unique_case_number)

            # Add custom properties
            proceeding["date"] = date

            # Add proceeding to the list
            proceedings.append(proceeding)

    print(f"Found a total of {total_proceedings} in all of DE")
    print(
        f"No courtcase-residences found for {no_courtcase_residences} out of those proceedings ({round(no_courtcase_residences / total_proceedings, 1)}%)"
    )

    print("Found", len(proceedings), "relevant proceedings")
    print(
        f"{nrw_duplicates} proceedings in NRW were discarded due to referring to the same court + case number"
    )

    # Bin proceedings by year
    by_year = defaultdict(list)

    for proceeding in proceedings:
        by_year[proceeding["date"].year].append(proceeding)

    # Bin proceedings by year and week
    by_year_and_week = defaultdict(lambda: defaultdict(list))
    by_year_and_week_count = defaultdict(lambda: defaultdict(int))

    for proceeding in proceedings:
        # Note: isocalendar week behaves weirdly between years
        calendar = proceeding["date"].isocalendar()
        by_year_and_week[calendar[0]][calendar[1]].append(proceeding)
        by_year_and_week_count[calendar[0]][calendar[1]] += 1

    # Construct dataframe
    df_by_week = pd.concat(
        {k: pd.Series(v).astype(float) for k, v in by_year_and_week_count.items()},
        axis=1,
    )

    df_by_week.index.name = "Woche"

    return df_by_week


def write_data_test():
    df = clear_data()
    # upload_dataframe(df, filename)


# If the file is executed directly, print cleaned data
if __name__ == "__main__":
    df = clear_data()
    print(df)
    with open("by_year_by_week.csv", "w") as fp:
        fp.write(df.to_csv(index=True))
