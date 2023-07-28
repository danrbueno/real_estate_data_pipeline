from pipelines.models.base import Base
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import backref, relationship

class Neighborhood(Base):
    __tablename__ = "neighborhoods"
    id = Column(Integer, primary_key=True)
    name = Column(String(length=200), nullable=False)

    city_id = Column(Integer, ForeignKey("cities.id"), nullable=False)
    city = relationship("City", back_populates="neighborhoods")

    properties = relationship("Property")

    def __init__(self, name, city):
        self.name = name
        self.city = city