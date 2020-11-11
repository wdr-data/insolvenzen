import pandas as pd

from insolvenzen.utils.source import CaseType
from insolvenzen.utils.storage import upload_dataframe
from . import private
from . import regular


def write_data_both():
    current_private = private.current()

    current_regulars = []
    for case_type in CaseType:
        current_regulars.append(regular.current(case_type))

    current_both = pd.concat([current_private] + current_regulars)
    upload_dataframe(current_both, f"both_current.csv")


if __name__ == "__main__":
    current_private = private.current()

    current_regulars = []
    for case_type in CaseType:
        current_regulars.append(regular.current(case_type))

    current_both = pd.concat([current_private] + current_regulars)

    with open(f"both_current.csv", "w") as fp:
        fp.write(current_both.to_csv(index=True))
