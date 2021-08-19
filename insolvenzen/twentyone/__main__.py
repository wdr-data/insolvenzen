"""
Run this file by using
python -m insolvenzen.twentyone
for local testing.
"""

from sys import argv, exit


from insolvenzen.twentyone import run_concurrent, analysis

if len(argv) != 2:
    print("Usage: pipenv run python -m insolvenzen.twentyone <command>")
    print("<command>: extract | analyze")
    exit(1)

command = argv[1]

if command == "extract":
    run_concurrent()
elif command == "analyze":
    analysis.run()
