import re
import datetime as dt

import dateparser

from insolvenzen.utils.types import JSON

RE_ZIPCODE = re.compile(r"\b\d{5}\b")
RE_DOB = re.compile(
    r"geb(?:\.|oren) (?:am )?(\d{2}\.\d{2}\.\d{4})\b",
    flags=re.IGNORECASE,
)
RE_PROCEEDING_TYPE = re.compile(
    r"(?<=\d_)([^\d]*)(?=.htm$)",
)
RE_PROCEEDING_DATE = re.compile(
    r"(\d{2}\.\d{2}\.\d{4})$",
)


def extract_features(case: JSON) -> dict:

    # Feature: ZIP code
    match = re.search(RE_ZIPCODE, case["description"])
    zipcode = match and match.group(0)

    # Feature: Date of birth
    match = re.search(RE_DOB, case["description"])
    try:
        dob = match and dateparser.parse(match.group(1), locales=["de"]).date()
    except AttributeError:
        dob = None

    # Feature: Kind (IK or IN)
    if "ik" in case["case_nr"].lower():
        kind = "ik"
    elif "in" in case["case_nr"].lower():
        kind = "in"
    else:
        kind = None

    # Feature: Type of proceeding
    match = re.search(RE_PROCEEDING_TYPE, case["file_name"])
    proceeding_type = match and match.group(0)

    # Feature: Date of proceeding
    match = re.search(RE_PROCEEDING_DATE, case["description"])
    proceeding_date = match and dateparser.parse(match.group(0), locales=["de"])

    # print(kind, zipcode, dob, proceeding_type)

    features = {
        "zipcode": zipcode,
        "date_of_birth": dob,
        "kind": kind,
        "proceeding_type": proceeding_type,
        "date_of_proceeding": proceeding_date,
    }

    # Update with pre-extracted features
    case["date_of_publication"] = case["date"]
    del case["date"]
    features.update(case)

    # Drop superfluous fields
    del features["description"]
    del features["url"]
    try:
        del features["_type"]
    except Exception:
        pass

    return features
