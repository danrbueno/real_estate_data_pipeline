from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, aliased
import os

# Connection with MariaDB
# Use environment variables to set up connection

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