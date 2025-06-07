from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TransactionEntity(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True)
    userId = Column(Integer, ForeignKey('users.id'), nullable=False)
    chargerId = Column(Integer, ForeignKey('chargers.id'), nullable=False)
    start = Column(DateTime, nullable=False)
    end = Column(DateTime, nullable=True)
    kWhConsumed = Column(Float, nullable=False)
    status = Column(String, nullable=False)
    paymentMethod = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String, nullable=False)

class UserEntity(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    tier = Column(String, nullable=False)
    createdAt = Column(DateTime, nullable=False)

class ChargerEntity(Base):
    __tablename__ = 'chargers'
    id = Column(Integer, primary_key=True)
    city = Column(String, nullable=False)
    location = Column(String, nullable=False)
    installedAt = Column(DateTime, nullable=False)