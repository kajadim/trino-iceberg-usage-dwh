"""DuckDB connection helpers for Iceberg-backed transformations."""

from contextlib import contextmanager
from typing import Generator

import duckdb


@contextmanager
def iceberg_connection(
    connection: duckdb.DuckDBPyConnection | None = None,
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Reuse a configured connection or create one with the Iceberg extension."""
    if connection is not None:
        yield connection
        return

    with duckdb.connect() as new_connection:
        new_connection.execute("INSTALL iceberg; LOAD iceberg;")
        yield new_connection
