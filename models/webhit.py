# models/webhit.py
from sqlalchemy import Column, Integer, String, Index, BigInteger, Sequence, func, DateTime

from models import Base


class WebHit(Base):
    __tablename__ = 'webhits'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestmp = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    client_id = Column(Integer, nullable=False)
    site_id = Column(Integer, nullable=False)
    ipaddress = Column(String(39), nullable=False)
    impression_id = Column(BigInteger, nullable=False)

    __table_args__ = (
        Index('idx_webhits_ip_client_site_timestmp', 'ipaddress', 'client_id', 'site_id', 'timestmp'),
        {'schema': 'edge_data'}
    )
