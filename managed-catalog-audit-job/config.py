import csv
from dataclasses import dataclass


DEFAULT_ENVS_FILE = "envs.csv"


@dataclass(frozen=True)
class Environment:
    name: str
    uri: str
    token: str


def load_environments(path=DEFAULT_ENVS_FILE):
    with open(path, newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        environments = []

        for row in reader:
            environments.append(
                Environment(
                    name=row["name"].strip(),
                    uri=row["uri"].strip(),
                    token=row["token"].strip(),
                )
            )

        return environments