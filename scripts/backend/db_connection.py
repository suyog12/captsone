from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"


def _load_env() -> dict[str, str]:
    """
    Load key=value pairs from the project .env file.

    Plain-text parser, no python-dotenv dependency, because we do not need
    interpolation or escaping for our simple values.
    """
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"Could not find .env file at {ENV_FILE}. "
            "Copy .env.example to .env and fill in your Postgres password."
        )

    out: dict[str, str] = {}
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out


def get_connection_url() -> str:
    """
    Build the SQLAlchemy URL string from .env values.

    Synchronous psycopg2 driver because the import scripts run as
    one-off batch jobs, not inside an async web server. The FastAPI
    backend will use asyncpg later through a separate connection helper.
    """
    env = _load_env()
    user = env["POSTGRES_USER"]
    pwd = env["POSTGRES_PASSWORD"]
    host = env.get("POSTGRES_HOST", "localhost")
    port = env.get("POSTGRES_PORT", "5432")
    db = env["POSTGRES_DB"]

    # Note: if the password contains special characters they need URL
    # encoding here. We URL-encode it explicitly to be safe regardless
    # of whether the password contains @, :, /, ?, or other special
    # characters.
    from urllib.parse import quote_plus
    pwd_enc = quote_plus(pwd)

    return f"postgresql+psycopg2://{user}:{pwd_enc}@{host}:{port}/{db}"


def get_schema() -> str:
    """Return the Postgres schema name from .env (default: recdash)."""
    return _load_env().get("POSTGRES_SCHEMA", "recdash")


def get_engine() -> Engine:
    """
    Create and return a SQLAlchemy engine pointed at the local Postgres.

    The engine is configured to set the search_path to our schema on every
    new connection so the import scripts can write 'INSERT INTO customers'
    instead of 'INSERT INTO recdash.customers'.
    """
    url = get_connection_url()
    schema = get_schema()

    engine = create_engine(
        url,
        # set the schema on every checkout so we never accidentally
        # write to public
        connect_args={"options": f"-c search_path={schema},public"},
        future=True,
    )
    return engine


def test_connection() -> None:
    """
    Quick connection sanity test. Run this directly to verify the .env
    settings work before kicking off the big imports.

    Usage:  python -m scripts.backend.db_connection
    """
    print("Testing Postgres connection ...")
    engine = get_engine()
    schema = get_schema()
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version()")).scalar()
        current_schema = conn.execute(
            text("SHOW search_path")
        ).scalar()
        n_tables = conn.execute(text(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{schema}'"
        )).scalar()
    print("  Postgres version :", version)
    print("  search_path      :", current_schema)
    print(f"  Tables in {schema:<10}:", n_tables)
    print("Connection OK.")


if __name__ == "__main__":
    test_connection()
