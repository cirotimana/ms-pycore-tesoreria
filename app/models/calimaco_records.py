from sqlalchemy import Column, Integer, String, TIMESTAMP, NUMERIC, TEXT, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CalimacoRecords(Base):
    __tablename__ = "calimaco_records"

    id = Column(Integer, primary_key=True, index=True)
    collector_id = Column(Integer, nullable=False)
    calimaco_id = Column(String(50), nullable=False)
    record_date = Column(TIMESTAMP, nullable=False)
    modification_date = Column(TIMESTAMP)
    status = Column(String(50), nullable=False)
    user_id = Column(String(50))
    amount = Column(NUMERIC(15, 2), nullable=False)
    external_id = Column(String(100))
    comments = Column(TEXT)
    created_at = Column(TIMESTAMP, server_default='NOW()')
    updated_at = Column(TIMESTAMP, server_default='NOW()')
    
    __table_args__ = (
        UniqueConstraint('collector_id', 'calimaco_id', 'status', name='unique_calimaco_collector_id_status'),
    )
