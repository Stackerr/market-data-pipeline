import polars as pl
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from src.database.connection import get_clickhouse_client

logger = logging.getLogger(__name__)


class ClickHouseStockMaster:
    """ClickHouse stock_master 테이블 관리 클래스"""

    def __init__(self):
        self.client = get_clickhouse_client()

    def create_table(self) -> bool:
        """stock_master 테이블 생성"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_master (
            symbol String,                          -- 종목코드
            name String,                           -- 종목명
            market String,                         -- 시장구분(KOSPI/KOSDAQ/ETF)
            listing_date Nullable(Date),           -- 상장일
            delisting_date Nullable(Date),         -- 상장폐지일
            is_active UInt8,                       -- 상장여부 (1: 상장, 0: 폐지)
            create_dt DateTime DEFAULT now(),       -- 생성일시
            update_dt DateTime DEFAULT now()        -- 수정일시
        ) ENGINE = ReplacingMergeTree(update_dt)
        ORDER BY symbol
        """

        try:
            self.client.command(create_table_sql)
            logger.info("stock_master table created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create stock_master table: {e}")
            return False

    def drop_table(self) -> bool:
        """stock_master 테이블 삭제 (테스트용)"""
        try:
            self.client.command("DROP TABLE IF EXISTS stock_master")
            logger.info("stock_master table dropped successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to drop stock_master table: {e}")
            return False

    def insert_stocks(self, stocks_df: pl.DataFrame) -> int:
        """종목 데이터 일괄 삽입"""
        if stocks_df.is_empty():
            logger.warning("Empty DataFrame provided for insertion")
            return 0

        # 필수 컬럼 검증
        required_columns = ['symbol', 'name', 'market']
        missing_columns = [col for col in required_columns if col not in stocks_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # 기본값 설정 (create_dt와 update_dt는 ClickHouse에서 자동 처리)
        processed_df = stocks_df.with_columns([
            pl.col('symbol').cast(pl.String),
            pl.col('name').cast(pl.String),
            pl.col('market').cast(pl.String),
            pl.when(pl.col('listing_date').is_null())
            .then(None)
            .otherwise(pl.col('listing_date')).alias('listing_date'),
            pl.when(pl.col('delisting_date').is_null())
            .then(None)
            .otherwise(pl.col('delisting_date')).alias('delisting_date'),
            pl.col('is_active').fill_null(1).cast(pl.UInt8)
        ])

        try:
            logger.info(f"Inserting {len(processed_df)} records to stock_master table")

            # Polars DataFrame을 Pandas로 변환하여 삽입
            pandas_df = processed_df.to_pandas()
            logger.debug(f"Pandas DataFrame shape: {pandas_df.shape}")
            logger.debug(f"Pandas DataFrame columns: {pandas_df.columns.tolist()}")

            result = self.client.insert_df('stock_master', pandas_df)
            logger.info(f"Insert result: {result}")

            inserted_count = len(processed_df)
            logger.info(f"Successfully inserted {inserted_count} stocks into stock_master")
            return inserted_count
        except Exception as e:
            logger.error(f"Failed to insert stocks: {e}")
            logger.error(f"Exception type: {type(e)}")
            raise

    def update_delisting_date(self, symbol: str, delisting_date: date) -> bool:
        """특정 종목의 상장폐지일 업데이트"""
        update_sql = f"""
        ALTER TABLE stock_master
        UPDATE delisting_date = '{delisting_date}',
               is_active = 0,
               update_dt = now()
        WHERE symbol = '{symbol}'
        """

        try:
            self.client.command(update_sql)
            logger.info(f"Updated delisting date for {symbol}: {delisting_date}")
            return True
        except Exception as e:
            logger.error(f"Failed to update delisting date for {symbol}: {e}")
            return False

    def get_stock_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """종목코드로 종목 정보 조회"""
        query = f"""
        SELECT *
        FROM stock_master
        WHERE symbol = '{symbol}'
        ORDER BY update_dt DESC
        LIMIT 1
        """

        try:
            result = self.client.query(query)
            if result.result_rows:
                # ClickHouse column_types는 (name, type) 튜플 리스트이거나 이름만 있을 수 있음
                if hasattr(result, 'column_names'):
                    columns = result.column_names
                elif result.column_types:
                    # column_types가 튜플 리스트인 경우
                    if isinstance(result.column_types[0], tuple):
                        columns = [col[0] for col in result.column_types]
                    else:
                        # column_types가 문자열 리스트인 경우
                        columns = result.column_types
                else:
                    logger.warning(f"Could not determine column names for symbol {symbol}")
                    return None

                return dict(zip(columns, result.result_rows[0]))
            return None
        except Exception as e:
            logger.error(f"Failed to get stock by symbol {symbol}: {e}")
            return None

    def get_active_stocks(self, market: Optional[str] = None) -> pl.DataFrame:
        """상장 종목 조회"""
        query = "SELECT * FROM stock_master WHERE is_active = 1"

        if market:
            query += f" AND market = '{market}'"

        query += " ORDER BY symbol"

        try:
            result = self.client.query_df(query)
            return pl.from_pandas(result) if not result.empty else pl.DataFrame()
        except Exception as e:
            logger.error(f"Failed to get active stocks: {e}")
            return pl.DataFrame()

    def get_delisted_stocks(self, market: Optional[str] = None) -> pl.DataFrame:
        """상장폐지 종목 조회"""
        query = "SELECT * FROM stock_master WHERE is_active = 0"

        if market:
            query += f" AND market = '{market}'"

        query += " ORDER BY delisting_date DESC, symbol"

        try:
            result = self.client.query_df(query)
            return pl.from_pandas(result) if not result.empty else pl.DataFrame()
        except Exception as e:
            logger.error(f"Failed to get delisted stocks: {e}")
            return pl.DataFrame()

    def get_stock_count(self) -> Dict[str, int]:
        """시장별 종목 수 통계"""
        query = """
        SELECT
            market,
            countIf(is_active = 1) as active_count,
            countIf(is_active = 0) as delisted_count,
            count(*) as total_count
        FROM stock_master
        GROUP BY market
        ORDER BY market
        """

        try:
            result = self.client.query_df(query)
            if result.empty:
                return {}

            stats = {}
            for _, row in result.iterrows():
                stats[row['market']] = {
                    'active': int(row['active_count']),
                    'delisted': int(row['delisted_count']),
                    'total': int(row['total_count'])
                }
            return stats
        except Exception as e:
            logger.error(f"Failed to get stock count: {e}")
            return {}

    def optimize_table(self) -> bool:
        """테이블 최적화"""
        try:
            self.client.command("OPTIMIZE TABLE stock_master FINAL")
            logger.info("stock_master table optimized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to optimize stock_master table: {e}")
            return False