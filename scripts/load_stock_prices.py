import polars as pl
import FinanceDataReader as fdr
from src.database.connection import db_connection
from src.storage.models import StockMaster
from src.storage.stock_price import StockPrice
from datetime import datetime, date
import logging
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tables():
    """모든 테이블 생성"""
    from src.storage.models import Base
    Base.metadata.create_all(bind=db_connection.engine)
    logger.info("All tables created successfully")

def get_stock_symbols():
    """DB에서 종목 목록 조회"""
    session = db_connection.get_session()
    try:
        stocks = session.query(StockMaster.symbol, StockMaster.name, StockMaster.is_active).all()
        logger.info(f"Found {len(stocks)} stocks in database")
        return stocks
    finally:
        session.close()

def load_stock_price_data(symbol, name, is_active=True):
    """개별 종목의 가격 데이터 적재"""
    session = db_connection.get_session()
    try:
        # 데이터 소스 결정 - 모든 종목에 대해 수정주가 사용
        if is_active:
            # 현재 상장 종목 - KRX:symbol 형식으로 수정주가 가져오기
            data_source = 'KRX'
            fdr_symbol = f'KRX:{symbol}'
        else:
            # 상장폐지 종목
            data_source = 'KRX-DELISTING'
            fdr_symbol = f'KRX-DELISTING:{symbol}'

        logger.info(f"Loading {symbol} ({name}) from {data_source}...")

        # FinanceDataReader로 데이터 가져오기
        try:
            df_pandas = fdr.DataReader(fdr_symbol)

            if df_pandas is None or len(df_pandas) == 0:
                logger.warning(f"No data for {symbol} ({name})")
                return 0

        except Exception as e:
            logger.warning(f"Failed to fetch data for {symbol} ({name}): {e}")
            return 0

        # Polars DataFrame으로 변환
        df = pl.from_pandas(df_pandas.reset_index())

        # 필요한 컬럼 매핑
        column_mapping = {
            'Date': 'trade_date',
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume': 'volume',
            'Amount': 'amount',
            'Change': 'change'
        }

        # 컬럼명 변경
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename({old_col: new_col})

        # 필수 컬럼 추가
        df = df.with_columns([
            pl.lit(symbol).alias('symbol'),
            pl.lit(data_source).alias('data_source'),
            pl.lit(datetime.now()).alias('created_at'),
            pl.lit(datetime.now()).alias('updated_at')
        ])

        # 필수 컬럼만 선택
        required_columns = ['symbol', 'trade_date', 'open_price', 'high_price', 'low_price',
                           'close_price', 'volume', 'amount', 'change', 'data_source',
                           'created_at', 'updated_at']

        # 존재하는 컬럼만 선택
        existing_columns = [col for col in required_columns if col in df.columns]
        df = df.select(existing_columns)

        # 배치 단위로 데이터베이스에 삽입
        batch_size = 1000
        inserted_count = 0

        for i in range(0, len(df), batch_size):
            batch = df.slice(i, min(batch_size, len(df) - i))

            # Dictionary 리스트로 변환
            records = batch.to_dicts()

            # SQLAlchemy ORM 객체 생성 및 삽입
            for record in records:
                # trade_date가 datetime이면 date로 변환
                if isinstance(record.get('trade_date'), datetime):
                    record['trade_date'] = record['trade_date'].date()

                stock_price = StockPrice(**record)
                session.merge(stock_price)  # upsert (insert or update)
                inserted_count += 1

            session.commit()
            logger.debug(f"Inserted batch {i//batch_size + 1} for {symbol}")

        logger.info(f"Successfully loaded {inserted_count} records for {symbol} ({name})")
        return inserted_count

    except Exception as e:
        session.rollback()
        logger.error(f"Error loading price data for {symbol} ({name}): {e}")
        return 0
    finally:
        session.close()

def load_all_stock_prices():
    """모든 종목의 가격 데이터 적재"""
    stocks = get_stock_symbols()
    total_inserted = 0

    logger.info(f"Starting to load price data for {len(stocks)} stocks...")

    for i, (symbol, name, is_active) in enumerate(stocks, 1):
        logger.info(f"[{i}/{len(stocks)}] Processing {symbol} ({name})")

        inserted = load_stock_price_data(symbol, name, is_active)
        total_inserted += inserted

        # 진행률 로깅
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(stocks)} stocks processed, {total_inserted} total records inserted")

    logger.info(f"Completed loading price data for all stocks. Total records: {total_inserted}")
    return total_inserted

def main():
    """메인 실행 함수"""
    logger.info("Starting stock price data loading...")

    # DB 연결 테스트
    try:
        session = db_connection.get_session()
        session.execute(text("SELECT 1"))
        session.close()
        logger.info("Database connection test successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return

    # 1. 테이블 생성
    create_tables()

    # 2. 모든 종목 가격 데이터 적재
    total_records = load_all_stock_prices()

    logger.info("Stock price data loading completed!")
    logger.info(f"Total price records loaded: {total_records}")

if __name__ == "__main__":
    main()