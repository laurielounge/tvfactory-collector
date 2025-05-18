from sqlalchemy import Column, Date, Integer

from models import Base


class TargetLastSiteResponse(Base):
    __tablename__ = 'last_site_response'
    __table_args__ = {'schema': 'infinitum_summaries'}

    date = Column(Date, nullable=False, primary_key=True)
    client_id = Column(Integer, nullable=False, primary_key=True)
    sitetag_id = Column(Integer, nullable=False, primary_key=True)
    hits = Column(Integer, nullable=True)
