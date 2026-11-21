"""Global limits and constants for the SQL sandpit service."""
from __future__ import annotations

import os

# Upload limits
MAX_UPLOAD_BYTES = 1 * 1024 * 1024  # 1 MiB CSV
MAX_CSV_ROWS = 200_000

# Concurrency: at most two rounds executing at the same time.
MAX_CONCURRENT_ROUNDS = 2

# Execution limits.
QUERY_TIMEOUT_SECONDS = 5.0
# Internal hard guard slightly above the advertised 5 s deadline; the
# supervisor also terminates the process at the advertised deadline.
HARD_DEADLINE_SECONDS = 5.5
MAX_RESULT_BYTES = 8 * 1024 * 1024  # 8 MiB result CSV
MAX_SQL_LENGTH = 100_000

# The only table visible inside a round.
TABLE_NAME = "observations"
TABLE_COLUMNS = ("station_id", "ts_utc", "temperature_c")

# How many rows a worker pulls / serialises per batch.
BATCH_SIZE = 2048

# Keep finished round metadata even after their slots are released. Results of
# finished rounds stay downloadable; both bounds are enforced so the 512 MiB
# container limit cannot be exhausted by many large successful results.
MAX_FINISHED_ROUNDS = 100
MAX_RETAINED_RESULT_BYTES = 96 * 1024 * 1024  # across all finished rounds

# Result cache: only complete, successful CSV results are kept. The cache is
# bounded both by entry count and by the total size of the cached CSVs.
MAX_CACHE_ENTRIES = 8
MAX_CACHE_BYTES = 16 * 1024 * 1024  # 16 MiB across all cached result CSVs


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default
