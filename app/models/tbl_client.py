from sqlalchemy import Column, Integer, String, DateTime, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class TblClient(Base):
    __tablename__ = "tbl_client"
    __table_args__ = {"schema": "sch_collectors"}

    id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, nullable=False)
    national_id_type = Column(String, nullable=False)
    national_id = Column(String, nullable=False)
    birthday = Column(Date, nullable=True)
    calimaco_user = Column(Numeric, nullable=True)
    mvt_id = Column(Numeric, nullable=True)
    calimaco_status = Column(String, nullable=True)
    created_at = Column(DateTime, server_default='NOW()')
    updated_at = Column(DateTime, server_default='NOW()', onupdate='NOW()')
    created_by = Column(Numeric, nullable=True)
    updated_by = Column(Numeric, nullable=True)
    

    
