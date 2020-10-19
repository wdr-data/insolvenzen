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


CASE_TYPE_HEADERS = {
    CaseType.VERFAHRENEROEFFNET: "Insolvenzverfahren",
    CaseType.ABWEISUNGMANGELSMASSE: "Abweisungen mangels Masse",
    CaseType.SICHERUNGSMASSNAHMEN: "Sicherungsmaßnahmen",
}


def history(case_type):
    cases, stats = filter_data(InsolvencyType.REGULAR)
    cases = cases[case_type]

    # Bin proceedings by year
    by_year = defaultdict(list)

    for case in cases:
        by_year[case["date"].year].append(case)

    # Bin proceedings by year and week
    by_week_count = defaultdict(lambda: defaultdict(int))
    by_year_and_week_count = defaultdict(lambda: defaultdict(int))

    calendar_today = dt.date.today().isocalendar()
    year_today = calendar_today[0]
    week_today = calendar_today[1]

    for case in cases:
        # Note: isocalendar week behaves weirdly between years
        calendar = case["date"].isocalendar()
        year = calendar[0]
        week = calendar[1]

        # ISO week as string for datawrapper
        if year == year_today or year == year_today - 1 and week >= week_today:
            subtitle = (
                "Unternehmen" if case["courtcase-is-company"] else "Selbstständige"
            )
            by_week_count[f"{year}W{week}"][
                f"{CASE_TYPE_HEADERS[case_type]} ({subtitle})"
            ] += 1

        by_year_and_week_count[year][week] += 1

    # Construct dataframes
    df_week_year = pd.concat(
        {k: pd.Series(v).astype(float) for k, v in by_year_and_week_count.items()},
        axis=1,
    )
    df_week_year.index.name = "Woche"

    df_week = pd.concat(
        {k: pd.Series(v).astype(float) for k, v in by_week_count.items()},
        axis=1,
    ).T.fillna(0.0)
    df_week.index.name = "Woche"
    df_week = df_week.reindex(sorted(df_week.columns), axis=1)

    return df_week, df_week_year


def districts(case_type):
    cases, stats = filter_data(InsolvencyType.REGULAR)
    cases = cases[case_type]

    # Filter for recent cases
    latest_data = max(p["date"] for p in cases)
    start_date = latest_data - dt.timedelta(days=30)
    last_30_days = [p for p in cases if p["date"] > start_date]

    # Group by district name
    by_district_name = defaultdict(lambda: defaultdict(int))

    for case in last_30_days:
        court = next(
            residence for residence in case["courtcase-residences"] if in_nrw(residence)
        )

        district_name = court["geolocation-street"]["street-gemeinde"][
            "gemeinde-kreis"
        ]["kreis-name"]
        district_name = normalize.district(district_name)
        num_inhabitants = inhabitants[district_name]

        by_district_name[district_name][CASE_TYPE_HEADERS[case_type]] += 1
        by_district_name[district_name][
            f"{CASE_TYPE_HEADERS[case_type]} pro 100.000 Einwohner"
        ] += (1 / num_inhabitants * 100_000)

    # Fill missing districts
    for district_name in inhabitants.keys():
        if district_name == "Gesamt":
            continue

        if district_name not in by_district_name:
            by_district_name[district_name] = {
                CASE_TYPE_HEADERS[case_type]: 0,
                f"{CASE_TYPE_HEADERS[case_type]} pro 100.000 Einwohner": 0.0,
            }

    # Convert to dataframe
    df = pd.DataFrame(by_district_name).T

    df[CASE_TYPE_HEADERS[case_type]] = df[CASE_TYPE_HEADERS[case_type]].astype(int)
    df.index.name = "Name"

    df = df.sort_index()

    return df


def current(case_type):
    cases, stats = filter_data(InsolvencyType.REGULAR)
    cases = cases[case_type]

    # Filter for recent proceedings
    latest_data = max(p["date"] for p in cases)

    date_7_days_ago = latest_data - dt.timedelta(days=7)
    date_14_days_ago = latest_data - dt.timedelta(days=14)

    last_7_days = len([p for p in cases if p["date"] > date_7_days_ago])
    the_7_days_before = len(
        [p for p in cases if date_7_days_ago >= p["date"] > date_14_days_ago]
    )
    try:
        percent_change = f"{signed(round((last_7_days - the_7_days_before) / the_7_days_before * 100))}%"
    except ZeroDivisionError:
        percent_change = "+∞%"

    df = pd.DataFrame(
        data={
            "Der letzten 7 Tage": {CASE_TYPE_HEADERS[case_type]: last_7_days},
            "Die 7 Tage davor": {CASE_TYPE_HEADERS[case_type]: the_7_days_before},
            "Veränderung": {CASE_TYPE_HEADERS[case_type]: percent_change},
        },
    )

    return df


def write_data_regular():
    districtses = []
    histories_by_week = []
    currents = []

    for case_type in CaseType:
        df = districts(case_type)
        upload_dataframe(df, f"regular_by_district_name_{case_type.value}.csv")

        districtses.append(df)

        df_week, df_year_week = history(case_type)
        upload_dataframe(df_year_week, f"regular_by_year_by_week_{case_type.value}.csv")
        upload_dataframe(df_week, f"regular_by_week_{case_type.value}.csv")

        histories_by_week.append(df_week)

        df = current(case_type)
        upload_dataframe(df, f"regular_current_{case_type.value}.csv")
        currents.append(df)

    df_districtses = pd.concat(districtses, axis=1)
    df_districtses.index.name = "Name"

    upload_dataframe(df_districtses, f"regular_by_district_name_merged.csv")

    df_histories_by_week = pd.concat(histories_by_week, axis=1)
    upload_dataframe(df_histories_by_week, f"regular_by_week_merged.csv")

    df_currents = pd.concat(currents)
    upload_dataframe(df_currents, f"regular_current_merged.csv")


# If the file is executed directly, print cleaned data
if __name__ == "__main__":
    districtses = []
    histories_by_week = []
    currents = []

    for case_type in CaseType:
        df = districts(case_type)
        with open(f"regular_by_district_name_{case_type.value}.csv", "w") as fp:
            fp.write(df.to_csv(index=True))

        districtses.append(df)

        df_week, df_year_week = history(case_type)
        with open(f"regular_by_year_by_week_{case_type.value}.csv", "w") as fp:
            fp.write(df_year_week.to_csv(index=True))
        with open(f"regular_by_week_{case_type.value}.csv", "w") as fp:
            fp.write(df_week.to_csv(index=True))

        histories_by_week.append(df_week)

        df = current(case_type)
        with open(f"regular_current_{case_type.value}.csv", "w") as fp:
            fp.write(df.to_csv(index=True))

        currents.append(df)

    df_districtses = pd.concat(districtses, axis=1)
    df_districtses.index.name = "Name"

    with open(f"regular_by_district_name_merged.csv", "w") as fp:
        fp.write(df_districtses.to_csv(index=True))

    df_histories_by_week = pd.concat(histories_by_week, axis=1)

    with open(f"regular_by_week_merged.csv", "w") as fp:
        fp.write(df_histories_by_week.to_csv(index=True))

    df_currents = pd.concat(currents)

    with open(f"regular_current_merged.csv", "w") as fp:
        fp.write(df_currents.to_csv(index=True))
