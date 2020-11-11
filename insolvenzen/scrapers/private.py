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
from insolvenzen.scrapers.common import filter_data, in_nrw, signed


def history():
    cases, stats = filter_data(InsolvencyType.PRIVATE)
    proceedings = cases[CaseType.VERFAHRENEROEFFNET]

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

    df_by_week["Durchschnitt 2018/2019"] = (df_by_week[2018] + df_by_week[2019]) / 2.0
    df_by_week = df_by_week.drop(columns=[2018, 2019])

    return df_by_week


def districts():
    cases, stats = filter_data(InsolvencyType.PRIVATE)
    proceedings = cases[CaseType.VERFAHRENEROEFFNET]

    # Filter for recent proceedings
    latest_data = max(p["date"] for p in proceedings)
    start_date = latest_data - dt.timedelta(days=30)
    last_30_days = [p for p in proceedings if p["date"] > start_date]

    # Group by district name
    by_district_name = defaultdict(int)

    for proceeding in last_30_days:
        court = next(
            residence
            for residence in proceeding["courtcase-residences"]
            if in_nrw(residence)
        )

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
    df = df.rename(columns={0: "pro 100.000 Einwohner"})

    return df


@lru_cache
def current():
    cases, stats = filter_data(InsolvencyType.PRIVATE)
    proceedings = cases[CaseType.VERFAHRENEROEFFNET]

    # Filter for recent proceedings
    latest_data = max(p["date"] for p in proceedings)

    date_7_days_ago = latest_data - dt.timedelta(days=7)
    date_14_days_ago = latest_data - dt.timedelta(days=14)

    last_7_days = len([p for p in proceedings if p["date"] > date_7_days_ago])
    the_7_days_before = len(
        [p for p in proceedings if date_7_days_ago >= p["date"] > date_14_days_ago]
    )
    try:
        percent_change = f"{signed(round((last_7_days - the_7_days_before) / the_7_days_before * 100))}%"
    except ZeroDivisionError:
        percent_change = "+∞%"

    df = pd.DataFrame(
        data={
            "Der letzten 7 Tage": {"Insolvenzverfahren": last_7_days},
            "Die 7 Tage davor": {"Insolvenzverfahren": the_7_days_before},
            "Veränderung": {"Insolvenzverfahren": percent_change},
        }
    )

    return df


def write_data_private():
    df = history()
    upload_dataframe(df, "private_by_year_by_week.csv")

    df = districts()
    upload_dataframe(df, "private_by_district_name.csv")

    df = current()
    upload_dataframe(df, "private_current.csv")


# If the file is executed directly, print cleaned data
if __name__ == "__main__":
    df = districts()
    with open("private_by_district_name.csv", "w") as fp:
        fp.write(df.to_csv(index=True))

    df = history()
    with open("private_by_year_by_week.csv", "w") as fp:
        fp.write(df.to_csv(index=True))

    df = current()
    with open("private_current.csv", "w") as fp:
        fp.write(df.to_csv(index=True))
