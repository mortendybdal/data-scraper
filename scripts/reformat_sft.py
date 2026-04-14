#!/usr/bin/env python3
"""Reformat CPT JSONL to SFT format (instruction/input/output)."""

import json
import sys
from pathlib import Path


def reformat(input_path: Path, output_path: Path) -> None:
    count = 0
    with open(input_path, "r") as fin, open(output_path, "w") as fout:
        for line in fin:
            item = json.loads(line)
            record = {
                "instruction": "",
                "input": "",
                "output": item["text"],
            }
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    print(f"Reformatted {count} records -> {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.jsonl> <output.jsonl>")
        sys.exit(1)
    reformat(Path(sys.argv[1]), Path(sys.argv[2]))
