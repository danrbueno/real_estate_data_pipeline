from pipelines.models.base import Base
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import backref, relationship

class City(Base):
    __tablename__ = "cities"
    id = Column(Integer, primary_key=True)
    name = Column(String(length=200), nullable=False)
    neighborhoods = relationship("Neighborhood")

    def __init__(self, name):
        self.name = name