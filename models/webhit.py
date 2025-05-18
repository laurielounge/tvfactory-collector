# models/webhit.py
from sqlalchemy import Column, Integer, String, Index, BigInteger, func, DateTime, TIMESTAMP, SmallInteger, \
    Date

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


class FinishedWebHit(Base):
    __tablename__ = 'webhits'
    __table_args__ = (
        Index('idx_webhits_ip_client_site_timestmp', 'ipaddress', 'client_id', 'site_id', 'timestmp'),
        Index('webhits_impression_id_index', 'impression_id'),
        {'schema': 'infinitum_raw_data'}
    )

    id = Column(BigInteger, primary_key=True)
    timestmp = Column(TIMESTAMP, nullable=False, default=func.current_timestamp(3))
    client_id = Column(Integer, nullable=False)
    site_id = Column(Integer, nullable=False)
    ipaddress = Column(String(39), nullable=False)
    impression_id = Column(BigInteger, nullable=True)
    views = Column(Integer, nullable=False, default=0)
    time_slot_id = Column(SmallInteger, nullable=True)
    date = Column(Date, nullable=True)
    summarised = Column(SmallInteger, nullable=False, default=0)
