from sqlalchemy import Column, Integer, String, Boolean, DateTime, Numeric, Date
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TblLiquidation(Base):
    __tablename__ = "tbl_liquidation"
    __table_args__ = {"schema": "sch_collectors"}

    id = Column(Integer, primary_key=True, index=True)
    collector_id = Column(Integer, nullable=False)
    liquidations_type = Column(Integer, nullable=False)##eliminar
    from_date = Column(Date, nullable=False)
    to_date = Column(Date, nullable=False)
    amount_collector = Column(Numeric(10, 2), nullable=False)
    amount_liquidation = Column(Numeric(10, 2), nullable=False)
    records_collector = Column(Integer, nullable=True)
    records_liquidation = Column(Integer, nullable=True)
    debit_amount_collector = Column(Numeric(10, 2), nullable=True)
    debit_amount_liquidation = Column(Numeric(10, 2), nullable=True)    
    credit_amount_collector = Column(Numeric(10, 2), nullable=True)
    credit_amount_liquidation = Column(Numeric(10, 2), nullable=True)
    
    unreconciled_credit_amount_collector = Column(Numeric(10, 2), nullable=True)
    unreconciled_credit_amount_liquidation = Column(Numeric(10, 2), nullable=True)
    unreconciled_debit_amount_collector = Column(Numeric(10, 2), nullable=True)
    unreconciled_debit_amount_liquidation = Column(Numeric(10, 2), nullable=True)    
    unreconciled_amount_collector = Column(Numeric(10, 2), nullable=False)
    unreconciled_amount_liquidation = Column(Numeric(10, 2), nullable=False)
    
    difference_amounts = Column(Numeric(10, 2), nullable=False)
    liquidations_state = Column(Boolean, nullable=False)##eliminar
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)
    activo = Column(Boolean, default=True, nullable=True)
    delete_at = Column(DateTime(timezone=True), nullable=True)
