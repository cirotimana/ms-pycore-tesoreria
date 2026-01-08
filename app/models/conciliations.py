from sqlalchemy import Column, Integer, String, Boolean, DateTime, Numeric, Date
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Conciliations(Base):
    __tablename__ = "conciliations"

    id = Column(Integer, primary_key=True, index=True)
    collector_id = Column(Integer, nullable=False)
    conciliations_type = Column(Integer, nullable=False)##eliminar
    from_date = Column(Date, nullable=False)
    to_date = Column(Date, nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    amount_collector = Column(Numeric(10, 2), nullable=False)
    difference_amounts = Column(Numeric(10, 2), nullable=False)
    records_calimaco = Column(Integer, nullable=True)
    records_collector = Column(Integer, nullable=True)
    unreconciled_records_calimaco = Column(Integer, nullable=True)
    unreconciled_records_collector = Column(Integer, nullable=True)
    unreconciled_amount_calimaco = Column(Numeric(10, 2), nullable=True)
    unreconciled_amount_collector = Column(Numeric(10, 2), nullable=True)
    conciliations_state = Column(Boolean, nullable=False)##eliminar
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_by = Column(Integer, nullable=True)
