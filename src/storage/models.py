from sqlalchemy import Column, String, Date, DateTime, Boolean, Text, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import date

Base = declarative_base()

class StockMaster(Base):
    """종목 마스터 테이블 - 상장/폐지 종목 모두 포함"""
    __tablename__ = 'stock_master'
    
    symbol = Column(String(20), primary_key=True, comment='종목코드')
    name = Column(String(100), nullable=False, comment='종목명')
    market = Column(String(20), nullable=False, comment='시장구분(KOSPI/KOSDAQ/KONEX)')
    sector = Column(String(50), comment='업종')
    industry = Column(String(100), comment='세부업종')
    
    # 상장/폐지 정보
    listing_date = Column(Date, comment='상장일')
    delisting_date = Column(Date, comment='상장폐지일')
    is_active = Column(Boolean, default=True, nullable=False, comment='상장여부')
    delisting_reason = Column(String(200), comment='상장폐지 사유')
    
    # 메타 정보
    created_at = Column(DateTime, server_default=func.now(), comment='생성일시')
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), comment='수정일시')
    data_source = Column(String(50), comment='데이터 출처')
    
    def __repr__(self):
        return f"<StockMaster({self.symbol}, {self.name}, active={self.is_active})>"