import datetime as dt
from functools import lru_cache

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

    # Filter irrelevant and duplicate values
for date, fil in files.items():
    total_proceedings += len(fil.get("verfahreneroeffnet", []))
    total_abweisung += len(fil.get("abweisungmangelsmasse", []))
    total_sicherung += len(fil.get("sicherungsmassnahmen", []))
    
print("verfahreneroeffnet", total_proceedings)
print("sicherungsmassnahmen", total_sicherung)
print("abweisungmangelsmasse", total_abweisung)

