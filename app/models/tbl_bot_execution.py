from sqlalchemy import Column, Integer, DateTime, Numeric, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class TblBotExecution(Base):
    __tablename__ = "tbl_bot_execution"
    __table_args__ = {"schema": "sch_collectors"}

    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, nullable=False)
    executed_at = Column(TIMESTAMP(timezone=True), nullable=False)
    total_processed_records = Column(Integer, nullable=False)
    total_detected_incidents = Column(Integer, nullable=False)
    created_at = Column(DateTime, server_default='NOW()')
    updated_at = Column(DateTime, server_default='NOW()', onupdate='NOW()')
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)
