from sqlalchemy import Column, String, DateTime, Float, Text, Index
from sqlalchemy.dialects.postgresql import UUID
import uuid
from src.database.connection import Base


class DelistedStock(Base):
    __tablename__ = "delisted_stocks"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(String(20), nullable=False, comment="종목코드")
    name = Column(String(100), nullable=False, comment="종목명")
    market = Column(String(20), nullable=False, comment="시장구분 (KOSPI/KOSDAQ)")
    secu_group = Column(String(20), comment="증권그룹")
    kind = Column(String(20), comment="종류")
    listing_date = Column(DateTime, nullable=False, comment="상장일")
    delisting_date = Column(DateTime, nullable=False, comment="상장폐지일")
    reason = Column(Text, comment="상장폐지사유")
    arrant_enforce_date = Column(DateTime, comment="관리종목지정일")
    arrant_end_date = Column(DateTime, comment="관리종목해제일")
    industry = Column(String(100), comment="업종")
    par_value = Column(Float, comment="액면가")
    listing_shares = Column(Float, comment="상장주식수")
    to_symbol = Column(String(20), comment="편입종목코드")
    to_name = Column(String(100), comment="편입종목명")

    # 인덱스 정의
    __table_args__ = (
        Index('idx_delisted_stocks_symbol', 'symbol'),
        Index('idx_delisted_stocks_market', 'market'),
        Index('idx_delisted_stocks_delisting_date', 'delisting_date'),
        Index('idx_delisted_stocks_listing_date', 'listing_date'),
        Index('idx_delisted_stocks_industry', 'industry'),
    )

    def __repr__(self):
        return f"<DelistedStock(symbol='{self.symbol}', name='{self.name}', market='{self.market}')>"