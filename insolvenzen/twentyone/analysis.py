from pathlib import Path
from os import environ

import pandas as pd


def load_data():
    if environ.get("USE_LOCAL_FILES"):
        paths = Path("extracted").rglob("*.csv")
        df_parts = []

        for path in sorted(paths):
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
    print(df.info())
