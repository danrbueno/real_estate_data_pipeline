from pipelines.models.base import Base
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Float, DateTime
from sqlalchemy.orm import backref, relationship

class Property(Base):
    __tablename__ = "properties"
    id = Column(Integer, primary_key=True)
    scraped_at = Column(DateTime, nullable=False)
    link = Column(String(length=200), nullable=False)
    title = Column(String(length=200), nullable=False)    
    price = Column(Float)
    useful_area = Column(Float)
    m2_value = Column(Float)
    rooms = Column(Integer)	
    parking = Column(Integer)
    condo_value = Column(Float)
    total_area = Column(Float)
    suites = Column(Integer)
    iptu = Column(Float)
    floor = Column(Integer)
        
    transaction_type_id = Column(Integer, ForeignKey("transaction_types.id"), nullable=False)
    transaction_type = relationship("TransactionType", back_populates="properties")

    neighborhood_id = Column(Integer, ForeignKey("neighborhoods.id"), nullable=False)
    neighborhood = relationship("Neighborhood", back_populates="properties")

    def __init__(self, 
                 scraped_at, 
                 link, 
                 title, 
                 price, 
                 useful_area, 
                 m2_value, 
                 rooms, 
                 parking, 
                 condo_value,
                 total_area,
                 suites,
                 iptu,
                 floor,
                 transaction_type, 
                 neighborhood):
        self.scraped_at = scraped_at
        self.link = link
        self.title = title
        self.price = price
        self.useful_area = useful_area
        self.m2_value = m2_value
        self.rooms = rooms
        self.parking = parking
        self.condo_value = condo_value
        self.total_area = total_area
        self.suites = suites
        self.iptu = iptu
        self.floor = floor
        self.transaction_type = transaction_type
        self.neighborhood = neighborhood