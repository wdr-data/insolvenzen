import datetime as dt
from functools import lru_cache
from collections import defaultdict

from insolvenzen.utils.source import InsolvencyType, load_source_file, list_files


@lru_cache
def get_files():
    # Download website
    filenames = list_files(InsolvencyType.REGULAR)
    files = {
        dt.date.fromisoformat(filename[:10]): load_source_file(
            InsolvencyType.REGULAR, filename
        )
        for filename in filenames
    }
    return files


files = get_files()

# Statistics overall
total_proceedings = 0
total_sicherung = 0
total_abweisung = 0

# Company => list of str
company_histories = defaultdict(tuple)


def process_category(fil, category):
    for item in fil.get(category, []):
        # No company names
        if "courtcase-company-names" not in item:
            continue

        # Category already recorded
        if category in company_histories[item["courtcase-company-names"][0]]:
            continue

        company_histories[item["courtcase-company-names"][0]] += (category,)


# Filter irrelevant and duplicate values
for date, fil in files.items():
    total_proceedings += len(fil.get("verfahreneroeffnet", []))
    total_abweisung += len(fil.get("abweisungmangelsmasse", []))
    total_sicherung += len(fil.get("sicherungsmassnahmen", []))

    process_category(fil, "verfahreneroeffnet")
    process_category(fil, "abweisungmangelsmasse")
    process_category(fil, "sicherungsmassnahmen")


company_history_stats = defaultdict(int)
for hist in company_histories.values():
    company_history_stats[hist] += 1

print(
    "\n".join(
        f"{key}: {val}"
        for key, val in sorted(company_history_stats.items(), key=(lambda t: t[1]))
    )
)

print("\n")
print("verfahreneroeffnet", total_proceedings)
print("sicherungsmassnahmen", total_sicherung)
print("abweisungmangelsmasse", total_abweisung)
