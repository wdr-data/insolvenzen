import re
from datetime import datetime, time
from functools import lru_cache

import requests
import pandas as pd
import pytz
import sentry_sdk

from insolvenzen.utils.storage import upload_dataframe

url = "https://example.com/"


@lru_cache
def get_data():
    # Download website
    response = requests.get(url)
    assert bool(response), "Laden der Beispiel-Seite fehlgeschlagen"

    # Parse into data frame
    df = pd.DataFrame()

    return df, response


def clear_data():
    df, response = get_data()

    # Clean up data here

    return df


def write_data_example():
    df = clear_data()
    filename = "example.csv"

    upload_dataframe(df, filename)


# If the file is executed directly, print cleaned data
if __name__ == "__main__":
    df = clear_data()
    # print(df)
    print(df.to_csv(index=False))
