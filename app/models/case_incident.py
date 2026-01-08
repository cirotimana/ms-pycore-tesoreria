from sqlalchemy import Column, Integer, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class Case_Incident(Base):
    __tablename__ = "case_incident"

    id = Column(Integer, primary_key=True, index=True)
    case_id = Column(Integer, nullable=False)
    data_json = Column(JSON, nullable=False)
    client_id = Column(Integer, nullable=True)
    channel_id = Column(Integer, nullable=True)

    
