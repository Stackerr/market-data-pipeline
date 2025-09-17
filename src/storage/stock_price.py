from sqlalchemy import Column, String, Date, DateTime, Numeric, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from .models import Base

class StockPrice(Base):
    """주식 가격 데이터 테이블 - 일별 시고저종 데이터"""
    __tablename__ = 'stock_price'

    # 복합 기본키: 종목코드 + 거래일
    symbol = Column(String(20), ForeignKey('stock_master.symbol'), primary_key=True, comment='종목코드')
    trade_date = Column(Date, primary_key=True, comment='거래일')

    # 가격 데이터 (소수점 2자리까지)
    open_price = Column(Numeric(12, 2), nullable=False, comment='시가')
    high_price = Column(Numeric(12, 2), nullable=False, comment='고가')
    low_price = Column(Numeric(12, 2), nullable=False, comment='저가')
    close_price = Column(Numeric(12, 2), nullable=False, comment='종가')

    # 거래 정보
    volume = Column(Numeric(15, 0), comment='거래량')
    amount = Column(Numeric(20, 0), comment='거래대금')

    # 기타 정보
    change = Column(Numeric(12, 2), comment='전일대비 변동액')
    change_pct = Column(Numeric(8, 4), comment='전일대비 변동률(%)')

    # 메타 정보
    created_at = Column(DateTime, server_default=func.now(), comment='생성일시')
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), comment='수정일시')
    data_source = Column(String(50), comment='데이터 출처')

    # 인덱스 최적화
    __table_args__ = (
        Index('idx_stock_price_symbol', 'symbol'),
        Index('idx_stock_price_date', 'trade_date'),
        Index('idx_stock_price_symbol_date', 'symbol', 'trade_date'),
    )

    def __repr__(self):
        return f"<StockPrice({self.symbol}, {self.trade_date}, {self.close_price})>"