from sqlalchemy import Column, Integer, String, DateTime, TIMESTAMP, Numeric
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class TblCase(Base):
    __tablename__ = "tbl_case"
    __table_args__ = {"schema": "sch_collectors"}

    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(Integer, nullable=False)
    capture_date = Column(TIMESTAMP(timezone=True), nullable=False, server_default='NOW()')
    description = Column(String, nullable=False)
    state_id = Column(Integer, nullable=False)
    close_date = Column(TIMESTAMP(timezone=True), nullable=True)
    close_detail = Column(String, nullable=True)
    close_evidence = Column(String, nullable=True)
    created_at = Column(DateTime, server_default='NOW()')
    updated_at = Column(DateTime, server_default='NOW()', onupdate='NOW()')
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)

    
