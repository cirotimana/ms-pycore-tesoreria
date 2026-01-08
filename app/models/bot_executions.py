from sqlalchemy import Column, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


    
class Bot_Executions(Base):
    __tablename__ = "bot_executions"

    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, nullable=False)
    executed_at = Column(DateTime, nullable=False)
    total_processed_records = Column(Integer, nullable=False)
    total_detected_incidents = Column(Integer, nullable=False)
