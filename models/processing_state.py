# models/processing_state.py
from datetime import datetime

from sqlalchemy import Column, String, BigInteger, DateTime

from models import Base


class ProcessingState(Base):
    """Elegant persistence of data processing state"""
    __tablename__ = 'processing_state'
    __table_args__ = {'schema': 'infinitum_raw_data'}

    entity_type = Column(String(50), primary_key=True)
    last_id = Column(BigInteger, nullable=False, default=0)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"<ProcessingState {self.entity_type}: {self.last_id}>"
