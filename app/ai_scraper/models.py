"""SQLAlchemy models for AI-scraper Postgres tables."""

from sqlalchemy import Column, Integer, String, Text

from db import Base


class MainPage(Base):
    """One downloaded pagination page of listing results."""

    __tablename__ = "tb_main_pages"

    id = Column(Integer, primary_key=True)
    page = Column(Integer, nullable=False)
    url = Column(String(length=500), nullable=False)
    html_content = Column(Text, nullable=False)
