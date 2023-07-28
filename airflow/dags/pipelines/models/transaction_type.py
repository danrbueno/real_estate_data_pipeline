from pipelines.models.base import Base
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import backref, relationship

class TransactionType(Base):
    __tablename__ = "transaction_types"
    id = Column(Integer, primary_key=True)
    name = Column(String(length=200), nullable=False)
    properties = relationship("Property")

    def __init__(self, name):
        self.name = name