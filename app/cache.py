"""Bounded in-memory cache for complete, successful result CSVs.

A cache key is the tuple ``(upload batch, station, canonical validated SQL)``.
Only results produced by a successful shared execution are ever inserted;
failures, timeouts, cancelled executions and oversized results never reach
this module.

The cache is deliberately independent of round metadata: evicting an entry
(or replacing the uploaded batch) never removes the downloadable result that
an already-finished round keeps.
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional

from .config import MAX_CACHE_BYTES, MAX_CACHE_ENTRIES


@dataclass(frozen=True)
class CacheKey:
    """Identity of a reusable result.

    ``sql_digest`` is the SHA-256 of the canonical (comment/whitespace
    normalised) validated SQL; the raw SQL itself is not stored in the key.
    """

    batch_id: int
    station_id: str
    sql_digest: str


@dataclass
class CacheEntry:
    key: CacheKey
    csv_bytes: bytes
    row_count: int
    result_bytes: int
    inserted_at: float
    last_used_at: float


class ResultCache:
    """Thread-safe LRU cache of result CSVs (max 8 entries / 16 MiB)."""

    def __init__(
        self,
        max_entries: int = MAX_CACHE_ENTRIES,
        max_bytes: int = MAX_CACHE_BYTES,
    ) -> None:
        self._lock = threading.RLock()
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._entries: "OrderedDict[CacheKey, CacheEntry]" = OrderedDict()
        self._bytes = 0
        # Incremented on every invalidation; executions started against an
        # older epoch cannot insert their (now stale) results.
        self._epoch = 0
        self._hits = 0
        self._misses = 0
        self._inserts = 0
        self._evictions = 0
        self._invalidations = 0

    # ------------------------------------------------------------------ reads
    def get(self, key: CacheKey) -> Optional[CacheEntry]:
        """Return a cached result and mark it as recently used."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                return None
            entry.last_used_at = time.monotonic()
            self._entries.move_to_end(key)
            self._hits += 1
            return entry

    # ----------------------------------------------------------------- writes
    def put(
        self,
        key: CacheKey,
        csv_bytes: bytes,
        row_count: int,
        result_bytes: int,
        epoch: Optional[int] = None,
    ) -> bool:
        """Insert a complete successful result, evicting LRU entries if needed.

        ``epoch`` binds the insert to the cache generation observed when the
        execution started. Returns ``False`` when the result is stale or
        cannot fit even in an empty cache.
        """
        size = len(csv_bytes)
        if size > self._max_bytes:
            return False
        now = time.monotonic()
        with self._lock:
            if epoch is not None and epoch != self._epoch:
                return False
            existing = self._entries.pop(key, None)
            if existing is not None:
                self._bytes -= len(existing.csv_bytes)
            # Make room *before* inserting so the limits hold after every put.
            while self._entries and (
                len(self._entries) >= self._max_entries
                or self._bytes + size > self._max_bytes
            ):
                _, evicted = self._entries.popitem(last=False)
                self._bytes -= len(evicted.csv_bytes)
                self._evictions += 1
            if self._bytes + size > self._max_bytes:
                return False
            self._entries[key] = CacheEntry(
                key=key,
                csv_bytes=csv_bytes,
                row_count=row_count,
                result_bytes=result_bytes,
                inserted_at=now,
                last_used_at=now,
            )
            self._bytes += size
            self._inserts += 1
            return True

    # ------------------------------------------------------------ invalidation
    def invalidate_all(self) -> int:
        """Drop every entry (used when a new upload batch becomes current)."""
        with self._lock:
            removed = len(self._entries)
            self._entries.clear()
            self._bytes = 0
            self._epoch += 1
            self._invalidations += removed
            return removed

    def epoch(self) -> int:
        """Current cache generation; pass it to :meth:`put` for stale-proofing."""
        with self._lock:
            return self._epoch

    # ------------------------------------------------------------------ stats
    def stats(self) -> dict:
        with self._lock:
            requests = self._hits + self._misses
            return {
                "entries": len(self._entries),
                "max_entries": self._max_entries,
                "bytes": self._bytes,
                "max_bytes": self._max_bytes,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(self._hits / requests, 4) if requests else 0.0,
                "inserts": self._inserts,
                "evictions": self._evictions,
                "invalidations": self._invalidations,
            }

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)
