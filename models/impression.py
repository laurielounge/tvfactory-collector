# models/impression.py
from sqlalchemy import Column, Integer, BigInteger, String, TIMESTAMP, Index, Sequence

from models import Base


class SourceImpression(Base):
    __tablename__ = 'impressions'
    id = Column(BigInteger, Sequence('impression_id_seq'), primary_key=True)
    timestmp = Column(TIMESTAMP, nullable=False, default='CURRENT_TIMESTAMP(3)')
    client_id = Column(Integer, nullable=False)
    booking_id = Column(Integer, nullable=False)
    creative_id = Column(Integer, nullable=False)
    ipaddress = Column(String(39), nullable=False)
    useragent = Column(String(500), nullable=True)

    __table_args__ = (
        Index('idx_impressions_ip_client_channel_timestmp', 'ipaddress', 'client_id', 'timestmp'),
        {'schema': 'mediaserver'}
    )
