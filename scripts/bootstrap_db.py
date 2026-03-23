"""Bootstrap the DuckDB schema (idempotent — safe to re-run)."""

from marketlens.config import settings
from marketlens.db import bootstrap_schema, get_connection


def main() -> None:
    conn = get_connection(settings.db_path)
    bootstrap_schema(conn)
    conn.close()
    print(f"Schema bootstrapped at {settings.db_path}")


if __name__ == "__main__":
    main()
