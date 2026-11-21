"""In-memory store of the latest uploaded observation data.

Each accepted upload gets a monotonically increasing ``batch_id``. The id is
part of every result-cache key and the cache is cleared when a new batch
becomes current, so results computed against an older CSV can never be
reused after a new upload.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional

from .csv_io import ParsedCsv, rows_to_columns


@dataclass
class Dataset:
    batch_id: int
    stations: list[str]
    total_rows: int
    # station_id -> column dict ready for the worker
    by_station: dict[str, dict[str, list]]
    filename: str


class DataStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._dataset: Optional[Dataset] = None
        self._next_batch_id = 1

    def replace(self, parsed: ParsedCsv, filename: str) -> Dataset:
        by_station: dict[str, dict[str, list]] = {}
        for station in parsed.stations:
            station_rows = [r for r in parsed.rows if r.station_id == station]
            by_station[station] = rows_to_columns(station_rows)
        with self._lock:
            batch_id = self._next_batch_id
            self._next_batch_id += 1
            dataset = Dataset(
                batch_id=batch_id,
                stations=parsed.stations,
                total_rows=parsed.total_rows,
                by_station=by_station,
                filename=filename,
            )
            self._dataset = dataset
        return dataset

    def current(self) -> Optional[Dataset]:
        with self._lock:
            return self._dataset

    def station_columns(self, station_id: str) -> Optional[dict[str, list]]:
        with self._lock:
            if self._dataset is None:
                return None
            return self._dataset.by_station.get(station_id)

    def reset(self) -> None:
        """Test helper: forget the current dataset."""
        with self._lock:
            self._dataset = None
