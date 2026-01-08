from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ConciliationFiles(Base):
    __tablename__ = "conciliation_files"

    id = Column(Integer, primary_key=True, index=True)
    conciliation_id = Column(Integer, nullable=False)
    conciliation_files_type = Column(Integer, nullable=False)
    file_path = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_by = Column(Integer, nullable=True)
