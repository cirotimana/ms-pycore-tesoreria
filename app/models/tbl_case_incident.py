from sqlalchemy import Column, Integer, JSON, Numeric, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class TblCaseIncident(Base):
    __tablename__ = "tbl_case_incident"
    __table_args__ = {"schema": "sch_collectors"}

    id = Column(Integer, primary_key=True, index=True)
    case_id = Column(Integer, nullable=False)
    data_json = Column(JSON, nullable=False)
    client_id = Column(Integer, nullable=True)
    channel_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default='NOW()')
    updated_at = Column(DateTime, server_default='NOW()', onupdate='NOW()')
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)

    
