# models/webhit.py
from sqlalchemy import Column, Integer, BigInteger, String, TIMESTAMP, Index, Sequence

from models import Base


class SourceWebHit(Base):
    __tablename__ = 'webhits'
    id = Column(BigInteger, Sequence('webhit_id_seq'), primary_key=True)
    timestmp = Column(TIMESTAMP, nullable=False, default='CURRENT_TIMESTAMP(3)')
    client_id = Column(Integer, nullable=False)
    site_id = Column(Integer, nullable=False)
    ipaddress = Column(String(39), nullable=False)
    impression_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index('idx_webhits_ip_client_site_timestmp', 'ipaddress', 'client_id', 'site_id', 'timestmp'),
        {'schema': 'mediaserver'}
    )
