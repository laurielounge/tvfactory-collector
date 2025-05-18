# models/impression.py
from sqlalchemy import Column, Integer, String, Index, func, DateTime

from models import Base


class Impression(Base):
    __tablename__ = 'impressions'
    id = Column(Integer, primary_key=True)
    timestmp = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    client_id = Column(Integer, nullable=False)
    booking_id = Column(Integer, nullable=False)
    creative_id = Column(Integer, nullable=False)
    ipaddress = Column(String(39), nullable=False)
    useragent = Column(String(500), nullable=True)

    __table_args__ = (
        Index('idx_impressions_ip_client_channel_timestmp', 'ipaddress', 'client_id', 'timestmp'),
        {'schema': 'infinitum_raw_data'}
    )
