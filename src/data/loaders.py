from __future__ import annotations
from pathlib import Path
from typing import Iterator, Optional
import pandas as pd

def jsonl_chunks(path: str | Path, chunksize: int) -> Iterator[pd.DataFrame]:
    yield from pd.read_json(path, lines=True, chunksize=chunksize)

def csv_chunks(path: str | Path, chunksize: int, encoding: str = "utf-8") -> Iterator[pd.DataFrame]:
    yield from pd.read_csv(path, chunksize=chunksize, encoding=encoding)
