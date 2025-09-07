import FinanceDataReader as fdr
import pandas as pd
from src.database.connection import db_connection, get_db_session
from src.models.delisted_stocks import DelistedStock
from sqlalchemy.exc import IntegrityError
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_tables():
    """데이터베이스 테이블 생성"""
    try:
        db_connection.create_tables()
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise


def load_delisted_stocks():
    """상장폐지 종목 데이터를 PostgreSQL에 적재"""
    try:
        # 1. FinanceDataReader로 데이터 가져오기
        logger.info("Fetching delisted stocks data from KRX...")
        delisted_df = fdr.StockListing('KRX-DELISTING')
        logger.info(f"Fetched {len(delisted_df)} delisted stocks")
        
        # 2. 데이터베이스 세션 생성
        session = db_connection.get_session()
        
        try:
            # 3. 기존 데이터 삭제 (재실행시)
            session.query(DelistedStock).delete()
            session.commit()
            logger.info("Existing delisted stocks data cleared")
            
            # 4. 데이터 변환 및 적재
            inserted_count = 0
            for _, row in delisted_df.iterrows():
                delisted_stock = DelistedStock(
                    symbol=row['Symbol'],
                    name=row['Name'],
                    market=row['Market'],
                    secu_group=row['SecuGroup'],
                    kind=row['Kind'],
                    listing_date=pd.to_datetime(row['ListingDate']),
                    delisting_date=pd.to_datetime(row['DelistingDate']),
                    reason=row['Reason'],
                    arrant_enforce_date=pd.to_datetime(row['ArrantEnforceDate']) if pd.notna(row['ArrantEnforceDate']) else None,
                    arrant_end_date=pd.to_datetime(row['ArrantEndDate']) if pd.notna(row['ArrantEndDate']) else None,
                    industry=row['Industry'],
                    par_value=row['ParValue'] if pd.notna(row['ParValue']) else None,
                    listing_shares=row['ListingShares'] if pd.notna(row['ListingShares']) else None,
                    to_symbol=row['ToSymbol'],
                    to_name=row['ToName']
                )
                
                session.add(delisted_stock)
                inserted_count += 1
                
                # 배치로 커밋 (성능 향상)
                if inserted_count % 1000 == 0:
                    session.commit()
                    logger.info(f"Inserted {inserted_count} records...")
            
            # 최종 커밋
            session.commit()
            logger.info(f"Successfully inserted {inserted_count} delisted stocks into database")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading data: {e}")
            raise
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"Error in load_delisted_stocks: {e}")
        raise


def verify_data():
    """데이터 적재 검증"""
    session = db_connection.get_session()
    try:
        total_count = session.query(DelistedStock).count()
        kospi_count = session.query(DelistedStock).filter(DelistedStock.market == 'KOSPI').count()
        kosdaq_count = session.query(DelistedStock).filter(DelistedStock.market == 'KOSDAQ').count()
        
        logger.info(f"Data verification:")
        logger.info(f"  Total records: {total_count}")
        logger.info(f"  KOSPI records: {kospi_count}")
        logger.info(f"  KOSDAQ records: {kosdaq_count}")
        
        # 최신 10개 종목 조회
        recent_stocks = session.query(DelistedStock).order_by(DelistedStock.delisting_date.desc()).limit(10).all()
        logger.info("Recent delisted stocks:")
        for stock in recent_stocks:
            logger.info(f"  {stock.symbol} - {stock.name} ({stock.market}) - {stock.delisting_date.date()}")
            
    finally:
        session.close()


if __name__ == "__main__":
    logger.info("Starting delisted stocks data loading process...")
    
    # 1. 테이블 생성
    create_tables()
    
    # 2. 데이터 적재
    load_delisted_stocks()
    
    # 3. 데이터 검증
    verify_data()
    
    logger.info("Delisted stocks data loading completed successfully!")