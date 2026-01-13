from sqlalchemy import Column, Integer, String, TIMESTAMP, NUMERIC, TEXT, UniqueConstraint, Numeric, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TblCalimacoRecord(Base):
    __tablename__ = "tbl_calimaco_record"
    __table_args__ = (
        UniqueConstraint('collector_id', 'calimaco_id', 'status', name='unique_calimaco_collector_id_status'),
        {"schema": "sch_collectors"}
    )

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
    calimaco_id_normalized = Column(String(50))
    created_at = Column(TIMESTAMP, server_default='NOW()')
    updated_at = Column(TIMESTAMP, server_default='NOW()', onupdate='NOW()')
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)
    activo = Column(Boolean, default=True, nullable=True)
    delete_at = Column(TIMESTAMP, nullable=True)
