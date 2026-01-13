from sqlalchemy import Column, Integer, String, DateTime, Numeric, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class TblBot(Base):
    __tablename__ = "tbl_bot"
    __table_args__ = {"schema": "sch_collectors"}

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    alert_type = Column(String, nullable=False)
    last_run = Column(TIMESTAMP(timezone=True), nullable=True)
    channel_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default='NOW()')
    updated_at = Column(DateTime, server_default='NOW()', onupdate='NOW()')
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)
