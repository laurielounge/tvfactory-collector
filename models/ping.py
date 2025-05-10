# model/ping.py
from sqlalchemy import Column, Integer, Date

from models import Base


class LastSiteResponse(Base):
    __tablename__ = 'last_site_response'

    date = Column(Date, nullable=False, primary_key=True)
    client_id = Column(Integer, nullable=False, primary_key=True)
    sitetag_id = Column(Integer, nullable=False, primary_key=True)
    hits = Column(Integer, nullable=True)

    __table_args__ = {'schema': 'edge_data'}


class LastImpressionResponse(Base):
    __tablename__ = 'last_impression_response'

    date = Column(Date, nullable=False, primary_key=True)
    client_id = Column(Integer, nullable=False, primary_key=True)
    booking_id = Column(Integer, nullable=False, primary_key=True)
    creative_id = Column(Integer, nullable=False, primary_key=True)
    hits = Column(Integer, nullable=True)

    __table_args__ = {'schema': 'edge_data'}
