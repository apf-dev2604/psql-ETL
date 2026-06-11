import csv
import os
from typing import Any, Dict, List


class CSVWriter:
    def __init__(self, path: str, fieldnames: List[str]):
        self.path = path
        self.fieldnames = fieldnames

    def write_row(self, row: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        exists = os.path.isfile(self.path)
        with open(self.path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames, extrasaction="ignore")
            if not exists:
                writer.writeheader()
            writer.writerow(row)
