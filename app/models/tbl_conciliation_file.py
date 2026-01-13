from sqlalchemy import Column, Integer, String, DateTime, Numeric
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TblConciliationFile(Base):
    __tablename__ = "tbl_conciliation_file"
    __table_args__ = {"schema": "sch_collectors"}

    id = Column(Integer, primary_key=True, index=True)
    conciliation_id = Column(Integer, nullable=False)
    conciliation_files_type = Column(Integer, nullable=False)
    file_path = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)
