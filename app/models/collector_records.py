from sqlalchemy import Column, Integer, String, TIMESTAMP, NUMERIC, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CollectorRecords(Base):
    __tablename__ = "collector_records"

    id = Column(Integer, primary_key=True, index=True)
    collector_id = Column(Integer, nullable=False)
    record_date = Column(TIMESTAMP, nullable=False)
    calimaco_id = Column(String(50), nullable=False)
    provider_id = Column(String(100))
    client_name = Column(String(255))
    amount = Column(NUMERIC(15, 2), nullable=False)
    provider_status = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, server_default='NOW()')
    updated_at = Column(TIMESTAMP, server_default='NOW()')
    
    __table_args__ = (
        UniqueConstraint('collector_id', 'calimaco_id', name='unique_collector_id_calimaco'),
    )
