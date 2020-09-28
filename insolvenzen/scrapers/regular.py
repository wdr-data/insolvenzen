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
from insolvenzen.utils.source import (
    InsolvencyType,
    CaseType,
)
from insolvenzen.data.inhabitants import inhabitants
from insolvenzen.data import normalize
from insolvenzen.scrapers.common import filter_data


def history(case_type):
    cases, stats = filter_data(InsolvencyType.REGULAR)
    proceedings = cases[case_type]

    # Bin proceedings by year
    by_year = defaultdict(list)

    for cases in proceedings:
        by_year[cases["date"].year].append(cases)

    # Bin proceedings by year and week
    by_year_and_week = defaultdict(lambda: defaultdict(list))
    by_year_and_week_count = defaultdict(lambda: defaultdict(int))

    for cases in proceedings:
        # Note: isocalendar week behaves weirdly between years
        calendar = cases["date"].isocalendar()
        by_year_and_week[calendar[0]][calendar[1]].append(cases)
        by_year_and_week_count[calendar[0]][calendar[1]] += 1

    # Construct dataframe
    df_by_week = pd.concat(
        {k: pd.Series(v).astype(float) for k, v in by_year_and_week_count.items()},
        axis=1,
    )

    df_by_week.index.name = "Woche"

    return df_by_week


def districts(case_type):
    cases, stats = filter_data(InsolvencyType.REGULAR)
    cases = cases[case_type]

    # Filter for recent proceedings
    start_date = dt.date.today() - dt.timedelta(days=30)
    last_30_days = [p for p in cases if p["date"] > start_date]

    # Group by district name
    by_district_name = defaultdict(int)

    for proceeding in last_30_days:
        court = proceeding["courtcase-residences"][0]

        district_name = court["geolocation-street"]["street-gemeinde"][
            "gemeinde-kreis"
        ]["kreis-name"]
        district_name = normalize.district(district_name)
        num_inhabitants = inhabitants[district_name]

        by_district_name[district_name] += 1 / num_inhabitants * 100_000

    # Fill missing districts
    for district_name in inhabitants.keys():
        if district_name == "Gesamt":
            continue

        if district_name not in by_district_name:
            by_district_name[district_name] = 0.0

    # Convert to dataframe
    df = pd.DataFrame([by_district_name]).T

    df.index.name = "Name"
    df = df.rename(columns={0: "Pro 100.000 Einwohner"})

    return df


def write_data_regular():
    for case_type in CaseType:
        df = districts(case_type)
        upload_dataframe(df, f"regular_by_district_name_{case_type.value}.csv")

        df = history(case_type)
        upload_dataframe(df, f"regular_by_year_by_week_{case_type.value}.csv")


# If the file is executed directly, print cleaned data
if __name__ == "__main__":
    for case_type in CaseType:
        df = districts(case_type)
        with open(f"regular_by_district_name_{case_type.value}.csv", "w") as fp:
            fp.write(df.to_csv(index=True))

        df = history(case_type)
        with open(f"regular_by_year_by_week_{case_type.value}.csv", "w") as fp:
            fp.write(df.to_csv(index=True))
