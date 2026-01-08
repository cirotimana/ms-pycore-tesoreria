from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class Cases(Base):
    __tablename__ = "cases"

    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(Integer, nullable=False)
    capture_date = Column(DateTime, nullable=False)
    description = Column(String, nullable=False)
    state_id = Column(Integer, nullable=False)
    close_date = Column(DateTime, nullable=True)
    close_detail = Column(String, nullable=True)
    close_evidence = Column(String, nullable=True)

    
