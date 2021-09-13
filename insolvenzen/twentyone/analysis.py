from pathlib import Path
from os import environ

import pandas as pd


def load_data():
    if environ.get("USE_LOCAL_FILES"):
        paths = Path("extracted").rglob("*.csv")
        df_parts = []

        for path in sorted(paths):
            if path.parts[-1] < "2021-08-22T03-47-42.jsonl.csv":
                continue

            print("Reading for analysis:", path)

            with open(path, "r", encoding="utf-8") as fp:
                df_part = pd.read_csv(fp, delimiter=",", dtype=str)
                df_parts.append(df_part)

            # if df is None:
            #     df = df_part
            # else:
            #     df = df.append(df_part, ignore_index=True)

        df = pd.concat(df_parts, ignore_index=True)

    else:
        raise NotImplementedError("Remote data not yet implemented")

    return df


def run():
    df = load_data()

    # Print dataframe sample and info
    pd.set_option("display.max_columns", 100)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_rows", 200)

    print(df)
    print("All:", len(df))

    print("Fingerprinted:", len(df.groupby("request_fingerprint").first()))

    print(
        "Eröffnungen all:",
        len(df[df["type_of_proceeding"] == "Eröffnungen"]),
    )
    print(
        "Eröffnungen fingerprinted:",
        len(
            df[df["type_of_proceeding"] == "Eröffnungen"]
            .groupby("request_fingerprint")
            .first()
        ),
    )
    print(
        "Eröffnungen filtered:",
        len(
            df[df["type_of_proceeding"] == "Eröffnungen"]
            .groupby(["case_nr", "date_of_publication"])
            .first()
        ),
    )
