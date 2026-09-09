"""Shared SQLAlchemy engine/session for AI-scraper tables, backed by Postgres.

Reads the same DB_* env vars used by app/airflow/dags/pipelines/models/base.py.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

import config  # noqa: F401  # ensures .env is loaded before reading DB_* vars below

connector = os.environ["DB_CONNECTION"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
host = os.environ["DB_HOST"]
port = os.environ["DB_PORT"]
schema = os.environ["DB_SCHEMA"]

engine = create_engine(
    f"{connector}://{user}:{password}@{host}:{port}/{schema}"
)

Session = sessionmaker(bind=engine)

Base = declarative_base()
