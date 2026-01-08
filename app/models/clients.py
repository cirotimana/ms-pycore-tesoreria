from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class Clients(Base):
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, nullable=False)
    national_id_type = Column(String, nullable=False)
    national_id = Column(String, nullable=False)
    birthday = Column(DateTime, nullable=True)
    calimaco_user = Column(Integer, nullable=True)
    mvt_id = Column(Integer, nullable=True)
    calimaco_status = Column(String, nullable=True)
    

    
