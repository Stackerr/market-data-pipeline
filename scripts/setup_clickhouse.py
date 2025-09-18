#!/usr/bin/env python3
"""
ClickHouse 로컬 설치 및 설정 스크립트
macOS에서 Homebrew를 사용한 설치 자동화
"""

import subprocess
import time
import logging
import os
from pathlib import Path
import polars as pl
from src.database.connection import db_connection
from src.storage.models import StockMaster
from src.storage.stock_price import StockPrice

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickHouseSetup:
    def __init__(self):
        self.clickhouse_client = None

    def install_clickhouse(self):
        """ClickHouse 설치 (macOS Homebrew)"""
        logger.info("Installing ClickHouse...")

        try:
            # Homebrew로 ClickHouse 설치
            subprocess.run([
                'brew', 'install', 'clickhouse'
            ], check=True)

            logger.info("ClickHouse installed successfully")

            # 서비스 시작
            subprocess.run([
                'brew', 'services', 'start', 'clickhouse'
            ], check=True)

            time.sleep(5)  # 서버 시작 대기
            logger.info("ClickHouse server started")

        except subprocess.CalledProcessError as e:
            logger.error(f"Installation failed: {e}")
            raise

    def check_clickhouse_running(self):
        """ClickHouse 서버 실행 확인"""
        try:
            result = subprocess.run([
                'clickhouse-client', '--query', 'SELECT 1'
            ], capture_output=True, text=True, check=True)

            if result.stdout.strip() == '1':
                logger.info("ClickHouse is running")
                return True

        except subprocess.CalledProcessError:
            logger.warning("ClickHouse is not running")
            return False

    def create_database_schema(self):
        """데이터베이스 스키마 생성"""
        logger.info("Creating ClickHouse schema...")

        # 데이터베이스 생성
        create_db_sql = """
        CREATE DATABASE IF NOT EXISTS market_data;
        """

        # 종목 마스터 테이블
        create_stock_master_sql = """
        CREATE TABLE IF NOT EXISTS market_data.stock_master (
            symbol String,
            name String,
            market String,
            sector String,
            industry String,
            listing_date Nullable(Date),
            delisting_date Nullable(Date),
            is_active UInt8,
            delisting_reason String,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY symbol;
        """

        # 가격 데이터 테이블 (파티셔닝)
        create_stock_price_sql = """
        CREATE TABLE IF NOT EXISTS market_data.stock_price (
            symbol String,
            trade_date Date,
            open_price Nullable(Float64),
            high_price Nullable(Float64),
            low_price Nullable(Float64),
            close_price Nullable(Float64),
            volume Nullable(UInt64),
            amount Nullable(UInt64),
            change Nullable(Float64),
            data_source String DEFAULT 'KRX',
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(trade_date)
        ORDER BY (symbol, trade_date);
        """

        # SQL 실행
        sqls = [create_db_sql, create_stock_master_sql, create_stock_price_sql]

        for sql in sqls:
            try:
                subprocess.run([
                    'clickhouse-client', '--query', sql
                ], check=True)
                logger.info("Schema created successfully")

            except subprocess.CalledProcessError as e:
                logger.error(f"Schema creation failed: {e}")
                raise

    def migrate_stock_master(self):
        """PostgreSQL에서 종목 마스터 데이터 마이그레이션"""
        logger.info("Migrating stock master data...")

        session = db_connection.get_session()
        try:
            # PostgreSQL에서 데이터 조회
            stocks = session.query(
                StockMaster.symbol,
                StockMaster.name,
                StockMaster.market,
                StockMaster.sector,
                StockMaster.industry,
                StockMaster.listing_date,
                StockMaster.delisting_date,
                StockMaster.is_active,
                StockMaster.delisting_reason
            ).all()

            logger.info(f"Found {len(stocks)} stocks to migrate")

            # CSV 파일로 임시 저장
            temp_file = "/tmp/stock_master.csv"

            with open(temp_file, 'w') as f:
                f.write("symbol,name,market,sector,industry,listing_date,delisting_date,is_active,delisting_reason\n")

                for stock in stocks:
                    line = f"{stock.symbol},{stock.name},{stock.market or ''}," \
                          f"{stock.sector or ''},{stock.industry or ''}," \
                          f"{stock.listing_date or ''},{stock.delisting_date or ''}," \
                          f"{1 if stock.is_active else 0},{stock.delisting_reason or ''}\n"
                    f.write(line)

            # ClickHouse로 데이터 삽입
            insert_cmd = [
                'clickhouse-client',
                '--query',
                f"INSERT INTO market_data.stock_master FORMAT CSV",
                '--input_format_skip_unknown_fields=1'
            ]

            with open(temp_file, 'r') as f:
                subprocess.run(insert_cmd, stdin=f, check=True)

            # 임시 파일 삭제
            os.unlink(temp_file)

            logger.info(f"Migrated {len(stocks)} stocks to ClickHouse")

        finally:
            session.close()

    def migrate_stock_prices_batch(self, batch_size=1000):
        """배치 단위로 가격 데이터 마이그레이션"""
        logger.info("Migrating stock price data...")

        session = db_connection.get_session()
        try:
            # 전체 레코드 수 확인
            total_count = session.query(StockPrice).count()
            logger.info(f"Total price records to migrate: {total_count:,}")

            batch_num = 0
            offset = 0

            while offset < total_count:
                batch_num += 1
                logger.info(f"Processing batch {batch_num}: {offset:,} - {offset + batch_size:,}")

                # 배치 데이터 조회
                batch_data = session.query(
                    StockPrice.symbol,
                    StockPrice.trade_date,
                    StockPrice.open_price,
                    StockPrice.high_price,
                    StockPrice.low_price,
                    StockPrice.close_price,
                    StockPrice.volume,
                    StockPrice.amount,
                    StockPrice.change,
                    StockPrice.data_source
                ).offset(offset).limit(batch_size).all()

                if not batch_data:
                    break

                # CSV 파일로 임시 저장
                temp_file = f"/tmp/stock_prices_batch_{batch_num}.csv"

                with open(temp_file, 'w') as f:
                    f.write("symbol,trade_date,open_price,high_price,low_price,close_price,volume,amount,change,data_source\n")

                    for row in batch_data:
                        line = f"{row.symbol},{row.trade_date}," \
                              f"{row.open_price or ''},{row.high_price or ''}," \
                              f"{row.low_price or ''},{row.close_price or ''}," \
                              f"{row.volume or ''},{row.amount or ''}," \
                              f"{row.change or ''},{row.data_source or 'KRX'}\n"
                        f.write(line)

                # ClickHouse로 삽입
                insert_cmd = [
                    'clickhouse-client',
                    '--query',
                    f"INSERT INTO market_data.stock_price FORMAT CSV",
                    '--input_format_skip_unknown_fields=1'
                ]

                with open(temp_file, 'r') as f:
                    subprocess.run(insert_cmd, stdin=f, check=True)

                # 임시 파일 삭제
                os.unlink(temp_file)

                offset += batch_size

                if batch_num % 10 == 0:
                    logger.info(f"Migrated {offset:,} / {total_count:,} records")

            logger.info(f"Migration completed: {total_count:,} records")

        finally:
            session.close()

    def create_optimized_views(self):
        """분석용 최적화된 뷰 생성"""
        logger.info("Creating optimized views...")

        # 일별 시장 요약 뷰
        daily_summary_view = """
        CREATE VIEW IF NOT EXISTS market_data.daily_market_summary AS
        SELECT
            trade_date,
            count() as total_stocks,
            avg(close_price) as avg_price,
            sum(volume) as total_volume,
            sum(amount) as total_amount
        FROM market_data.stock_price
        WHERE close_price > 0
        GROUP BY trade_date
        ORDER BY trade_date;
        """

        # 종목별 최신 가격 뷰
        latest_prices_view = """
        CREATE VIEW IF NOT EXISTS market_data.latest_prices AS
        SELECT
            sp.symbol,
            sm.name,
            sp.trade_date,
            sp.close_price,
            sp.volume,
            sm.market
        FROM market_data.stock_price sp
        INNER JOIN market_data.stock_master sm ON sp.symbol = sm.symbol
        WHERE sp.trade_date = (
            SELECT max(trade_date)
            FROM market_data.stock_price sp2
            WHERE sp2.symbol = sp.symbol
        );
        """

        views = [daily_summary_view, latest_prices_view]

        for view_sql in views:
            try:
                subprocess.run([
                    'clickhouse-client', '--query', view_sql
                ], check=True)
                logger.info("Views created successfully")

            except subprocess.CalledProcessError as e:
                logger.error(f"View creation failed: {e}")

    def verify_migration(self):
        """마이그레이션 결과 검증"""
        logger.info("Verifying migration...")

        # 종목 수 확인
        result = subprocess.run([
            'clickhouse-client', '--query',
            'SELECT count() FROM market_data.stock_master'
        ], capture_output=True, text=True, check=True)

        stock_count = int(result.stdout.strip())
        logger.info(f"ClickHouse stock count: {stock_count}")

        # 가격 데이터 수 확인
        result = subprocess.run([
            'clickhouse-client', '--query',
            'SELECT count() FROM market_data.stock_price'
        ], capture_output=True, text=True, check=True)

        price_count = int(result.stdout.strip())
        logger.info(f"ClickHouse price count: {price_count:,}")

        # 샘플 쿼리 성능 테스트
        logger.info("Running performance test...")
        start_time = time.time()

        subprocess.run([
            'clickhouse-client', '--query',
            """SELECT symbol, avg(close_price), count()
               FROM market_data.stock_price
               WHERE trade_date >= '2023-01-01'
               GROUP BY symbol
               LIMIT 10"""
        ], capture_output=True, text=True, check=True)

        elapsed = time.time() - start_time
        logger.info(f"Sample query executed in {elapsed:.3f} seconds")

def main():
    """메인 실행 함수"""
    setup = ClickHouseSetup()

    try:
        # 1. ClickHouse 설치
        if not setup.check_clickhouse_running():
            setup.install_clickhouse()

        # 2. 스키마 생성
        setup.create_database_schema()

        # 3. 데이터 마이그레이션
        setup.migrate_stock_master()
        setup.migrate_stock_prices_batch()

        # 4. 최적화된 뷰 생성
        setup.create_optimized_views()

        # 5. 검증
        setup.verify_migration()

        logger.info("ClickHouse setup completed successfully!")
        logger.info("Access ClickHouse: clickhouse-client")
        logger.info("Web UI: http://localhost:8123/play")

    except Exception as e:
        logger.error(f"Setup failed: {e}")
        raise

if __name__ == "__main__":
    main()