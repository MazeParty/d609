"""Utility to convert newline separated JSON objects to valid JSON arrays."""

from __future__ import annotations

import json
import re
from pathlib import Path


def _clean_file(path: Path) -> None:
    """Rewrite the file as a JSON array if it isn't already."""

    # Read and trim whitespace so we can easily detect existing arrays
    content = path.read_text().strip()
    if not content:
        return

    # Even if the file already looks like a JSON array we attempt to reparse
    # it to handle potential corruption.

    # Extract every JSON object from the text. This handles files where
    # objects are separated by newlines, concatenated, or interspersed with
    # random characters.
    objs = []
    for match in re.finditer(r"\{[^{}]*\}", content):
        obj_text = match.group(0)
        try:
            json.loads(obj_text)
            objs.append(obj_text)
        except json.JSONDecodeError:
            continue

    if not objs:
        return

    new_content = "[" + ",".join(objs) + "]"

    path.write_text(new_content)


def clean_json() -> None:
    """Clean all landing zone JSON files in this repository."""

    landing_dirs = [
        Path("customer") / "landing",
        Path("accelerometer") / "landing",
        Path("step_trainer") / "landing",
    ]

    for directory in landing_dirs:
        if not directory.exists():
            continue
        for file in directory.iterdir():
            if file.suffix.lower() != ".json":
                continue
            _clean_file(file)


if __name__ == "__main__":
    clean_json()

