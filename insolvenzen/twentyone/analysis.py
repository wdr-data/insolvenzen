from pathlib import Path
from os import environ

import pandas as pd


def load_data():
    if environ.get("USE_LOCAL_FILES"):
        paths = Path("extracted").rglob("*.csv")
        df_parts = []

        for path in sorted(paths):
            # if path.parts[-1] < "2021-08-11T14-54-35.jsonl.csv":
            #     continue

            # test = [
            #     "2021-03-14T04-06-39.jsonl.csv",
            #     "2021-10-20T00-26-46.jsonl.csv",
            # ]
            # if path.parts[-1] not in test:
            #     continue

            print("Reading for analysis:", path)

            with open(path, "r", encoding="utf-8") as fp:
                df_part = pd.read_csv(fp, delimiter=",", dtype=str)
                df_part["date_of_publication"] = pd.to_datetime(
                    df_part["date_of_publication"],
                    format="%Y-%m-%d",
                )
                df_part["date_of_proceeding"] = pd.to_datetime(
                    df_part["date_of_proceeding"],
                    format="%Y-%m-%d",
                    errors="coerce",
                )
                df_part["date_of_birth"] = pd.to_datetime(
                    df_part["date_of_birth"],
                    format="%Y-%m-%d",
                    errors="coerce",
                )
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

    # Print dataframe sample and info
    pd.set_option("display.max_columns", 100)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_rows", 200)

    df = load_data()

    df = df[df["federal_state"].isin(["Nordrhein-Westfalen", "nw"])]

    # ik = privat, in = geschaeftlich
    df = df[df["kind"] == "ik"]

    # print(df)
    print("All:", len(df))

    df = df.groupby("description_hash", as_index=False).first()
    print("Fingerprinted:", len(df))

    # print(
    #     "Eröffnungen all:",
    #     len(df[df["type_of_proceeding"] == "Eröffnungen"]),
    # )
    # print(
    #     "Eröffnungen filtered:",
    #     len(
    #         df[df["type_of_proceeding"] == "Eröffnungen"]
    #         .groupby(["case_nr", "date_of_publication"])
    #         .first()
    #     ),
    # )

    ## Filter Eröffnungen
    # print(df["type_of_proceeding"].value_counts())
    df_new = df[
        df["type_of_proceeding"].isin(
            [
                "Eröffnungen",
                "Eroeffnung",
                "Eroeffnung_Insolvenzverfahren",
            ]
        )
    ]
    print(
        "Eröffnungen fingerprinted:",
        len(df_new),
    )

    ## Testing stuff

    # df_test = df_new[df_new["date_of_publication"] == dt.datetime(2021, 10, 11)]
    # print(df_test)
    # df_test.to_csv("test.csv", index=False)

    ## Daily analysis
    # print(
    #     df_new[["date_of_publication", "description_hash"]]
    #     .groupby(pd.Grouper(key="date_of_publication", freq="D"))
    #     .count()
    #     .tail(60)
    # )

    ## Weekly analysis
    print(
        df_new[["date_of_publication", "description_hash"]]
        .groupby(pd.Grouper(key="date_of_publication", freq="W-MON", label="left"))
        .count()
    )

    # print(
    #     df_new[["date_of_proceeding", "description_hash"]]
    #     .groupby(pd.Grouper(key="date_of_proceeding", freq="W-MON", label="left"), dropna=True)
    #     .count()
    # )
