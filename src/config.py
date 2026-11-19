"""Project paths and Iceberg catalog configuration."""

import os
from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
SAMPLE_EVENTS_PATH = PROJECT_ROOT / "data" / "sample" / "raw_query_events.jsonl"
SEED_DIR = PROJECT_ROOT / "data" / "seed"

CATALOG_NAME = "default"
NAMESPACE = "meta_dwh"
RAW_QUERY_EVENTS_TABLE = f"{NAMESPACE}.raw_query_events"
DIM_USER_TABLE = f"{NAMESPACE}.dim_user"
DIM_TEAM_TABLE = f"{NAMESPACE}.dim_team"
DIM_TABLE_TABLE = f"{NAMESPACE}.dim_table"
FACT_TABLE_ACCESS_TABLE = f"{NAMESPACE}.fact_table_access"
AGG_TABLE_USAGE_DAILY_TABLE = f"{NAMESPACE}.agg_table_usage_daily"
USER_TEAM_MAPPING_PATH = SEED_DIR / "user_team_mapping.csv"
TABLE_CATALOG_PATH = SEED_DIR / "table_catalog.csv"
ICEBERG_SOURCE_TYPES = frozenset({"iceberg", "s3"})


def load_project_catalog() -> Catalog:
    """Load the catalog configured in the repository's .pyiceberg.yaml."""
    os.environ.setdefault("PYICEBERG_HOME", str(PROJECT_ROOT))
    return load_catalog(CATALOG_NAME)
