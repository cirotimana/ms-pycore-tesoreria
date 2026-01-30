from sqlalchemy import Column, Integer, String, TIMESTAMP, NUMERIC, UniqueConstraint, Numeric, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TblCollectorRecord(Base):
    __tablename__ = "tbl_collector_record"
    __table_args__ = (
        UniqueConstraint('collector_id', 'calimaco_id', 'amount', name='unique_collector_id_calimaco'),
        {"schema": "sch_collectors"}
    )

    id = Column(Integer, primary_key=True, index=True)
    collector_id = Column(Integer, nullable=False)
    record_date = Column(TIMESTAMP, nullable=False)
    calimaco_id = Column(String(50), nullable=False)
    provider_id = Column(String(100))
    client_name = Column(String(255))
    amount = Column(NUMERIC(15, 2), nullable=False)
    provider_status = Column(String(50), nullable=False)
    calimaco_id_normalized = Column(String(50))
    created_at = Column(TIMESTAMP, server_default='NOW()')
    updated_at = Column(TIMESTAMP, server_default='NOW()', onupdate='NOW()')
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)
    activo = Column(Boolean, default=True, nullable=True)
    delete_at = Column(TIMESTAMP, nullable=True)
