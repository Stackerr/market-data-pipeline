from src.database.connection import db_connection
from src.storage.models import StockMaster
from sqlalchemy import func, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_stock_data():
    """주식 마스터 데이터 현황 확인"""
    session = db_connection.get_session()
    try:
        print("="*60)
        print("📊 STOCK MASTER 데이터 현황")
        print("="*60)
        
        # 1. 전체 종목 수
        total_count = session.query(StockMaster).count()
        print(f"📈 전체 종목 수: {total_count:,}개")
        
        # 2. 상장/폐지 현황
        active_count = session.query(StockMaster).filter(StockMaster.is_active == True).count()
        inactive_count = session.query(StockMaster).filter(StockMaster.is_active == False).count()
        
        print(f"✅ 상장 종목: {active_count:,}개")
        print(f"❌ 상장폐지 종목: {inactive_count:,}개")
        
        # 3. 시장별 현황 (상장)
        print("\n📊 시장별 현황 (상장 종목)")
        print("-" * 40)
        market_stats = session.query(
            StockMaster.market, 
            func.count(StockMaster.symbol)
        ).filter(
            StockMaster.is_active == True
        ).group_by(StockMaster.market).all()
        
        for market, count in market_stats:
            print(f"  {market}: {count:,}개")
        
        # 4. 시장별 현황 (상장폐지)
        print("\n📊 시장별 현황 (상장폐지 종목)")
        print("-" * 40)
        delisted_stats = session.query(
            StockMaster.market, 
            func.count(StockMaster.symbol)
        ).filter(
            StockMaster.is_active == False
        ).group_by(StockMaster.market).all()
        
        for market, count in delisted_stats:
            print(f"  {market}: {count:,}개")
        
        # 5. 데이터 소스별 현황
        print("\n📊 데이터 소스별 현황")
        print("-" * 40)
        source_stats = session.query(
            StockMaster.data_source, 
            func.count(StockMaster.symbol)
        ).group_by(StockMaster.data_source).all()
        
        for source, count in source_stats:
            print(f"  {source}: {count:,}개")
        
        # 6. 상장일/폐지일 정보 현황
        print("\n📊 날짜 정보 현황")
        print("-" * 40)
        
        # 상장일이 있는 종목 수
        listing_date_count = session.query(StockMaster).filter(
            StockMaster.listing_date.isnot(None)
        ).count()
        print(f"  상장일 정보 있음: {listing_date_count:,}개")
        
        # 상장폐지일이 있는 종목 수  
        delisting_date_count = session.query(StockMaster).filter(
            StockMaster.delisting_date.isnot(None)
        ).count()
        print(f"  상장폐지일 정보 있음: {delisting_date_count:,}개")
        
        # 7. 최신 상장 종목 (상위 10개)
        print("\n📊 최신 데이터 샘플 (상장 종목 10개)")
        print("-" * 60)
        recent_active = session.query(StockMaster).filter(
            StockMaster.is_active == True
        ).limit(10).all()
        
        for stock in recent_active:
            listing_info = f" (상장: {stock.listing_date})" if stock.listing_date else ""
            print(f"  {stock.symbol} - {stock.name} [{stock.market}]{listing_info}")
        
        # 8. 상장폐지 종목 샘플 (상위 10개)
        print("\n📊 상장폐지 종목 샘플 (10개)")
        print("-" * 60)
        delisted_sample = session.query(StockMaster).filter(
            StockMaster.is_active == False
        ).filter(
            StockMaster.delisting_date.isnot(None)
        ).order_by(StockMaster.delisting_date.desc()).limit(10).all()
        
        for stock in delisted_sample:
            delisting_info = f" (폐지: {stock.delisting_date})"
            reason = f" - {stock.delisting_reason}" if stock.delisting_reason else ""
            print(f"  {stock.symbol} - {stock.name} [{stock.market}]{delisting_info}{reason}")
        
        print("\n" + "="*60)
        
    except Exception as e:
        logger.error(f"Error checking stock data: {e}")
        raise
    finally:
        session.close()

def check_missing_data():
    """누락된 데이터 확인"""
    session = db_connection.get_session()
    try:
        print("\n🔍 데이터 누락 현황")
        print("="*60)
        
        # 상장 종목 중 상장일이 없는 경우
        active_no_listing_date = session.query(StockMaster).filter(
            StockMaster.is_active == True,
            StockMaster.listing_date.is_(None)
        ).count()
        
        print(f"❗ 상장 종목 중 상장일 누락: {active_no_listing_date:,}개")
        
        # 상장폐지 종목 중 폐지일이 없는 경우
        inactive_no_delisting_date = session.query(StockMaster).filter(
            StockMaster.is_active == False,
            StockMaster.delisting_date.is_(None)
        ).count()
        
        print(f"❗ 상장폐지 종목 중 폐지일 누락: {inactive_no_delisting_date:,}개")
        
    except Exception as e:
        logger.error(f"Error checking missing data: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    check_stock_data()
    check_missing_data()