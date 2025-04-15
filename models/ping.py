# model/ping.py
from sqlalchemy import Column, Integer, Date

from models import Base


class SourceLastSiteResponse(Base):
    __tablename__ = 'last_site_response'

    date = Column(Date, nullable=False, primary_key=True)
    client_id = Column(Integer, nullable=False, primary_key=True)
    sitetag_id = Column(Integer, nullable=False, primary_key=True)
    hits = Column(Integer, nullable=True)

    __table_args__ = {'schema': 'mediaserver'}
