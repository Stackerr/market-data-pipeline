#!/usr/bin/env python3
"""
기존 Parquet 파일에서 상장폐지 종목 데이터를 읽어 ClickHouse에 업데이트
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import polars as pl

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.clickhouse.stock_master import ClickHouseStockMaster

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_delisted_from_parquet(parquet_path: Path) -> pl.DataFrame:
    """Parquet 파일에서 상장폐지 종목 데이터 로드"""
    logger.info(f"Loading delisted data from: {parquet_path}")

    if not parquet_path.exists():
        logger.error(f"Parquet file not found: {parquet_path}")
        return pl.DataFrame()

    try:
        df = pl.read_parquet(parquet_path)
        logger.info(f"✅ Loaded {len(df)} records from parquet")
        logger.info(f"Columns: {df.columns}")
        return df
    except Exception as e:
        logger.error(f"Failed to load parquet file: {e}")
        return pl.DataFrame()


def map_delisted_columns(df: pl.DataFrame) -> pl.DataFrame:
    """상장폐지 데이터 컬럼을 ClickHouse 스키마에 맞게 매핑"""
    logger.info("Mapping columns to ClickHouse schema...")

    # 컬럼명 매핑
    column_mapping = {
        'company_code': 'symbol',
        'company_name': 'name',
        'delisting_date': 'delisting_date',
        'delisting_reason': 'delisting_reason',
        'remarks': 'remarks'
    }

    # 기존 컬럼명 확인 및 매핑
    mapped_df = df
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            mapped_df = mapped_df.rename({old_col: new_col})

    # ClickHouse 스키마에 맞는 컬럼 추가/변환
    if 'symbol' in mapped_df.columns:
        # 종목코드 정리 (6자리 숫자만)
        mapped_df = mapped_df.with_columns(
            pl.col('symbol').cast(pl.String).str.replace_all(r'[^\d]', '').str.slice(0, 6).alias('symbol')
        )

        # 6자리가 아닌 종목코드 필터링
        mapped_df = mapped_df.filter(pl.col('symbol').str.len_chars() == 6)

    # 필수 컬럼 추가
    mapped_df = mapped_df.with_columns([
        pl.when(pl.col('name').is_null()).then(pl.lit("Unknown")).otherwise(pl.col('name')).alias('name'),
        pl.lit(None).alias('market').cast(pl.String),  # 시장 정보는 나중에 추론
        pl.lit(None).alias('listing_date').cast(pl.Date),
        pl.when(pl.col('delisting_date').is_null()).then(pl.date(2020, 1, 1)).otherwise(pl.col('delisting_date')).alias('delisting_date'),
        pl.lit(0).alias('is_active').cast(pl.UInt8)  # 상장폐지 종목은 모두 비활성
    ])

    # 최종 컬럼 선택 (ClickHouse stock_master 스키마에 맞춤)
    final_df = mapped_df.select([
        'symbol',
        'name',
        'market',
        'listing_date',
        'delisting_date',
        'is_active'
    ])

    logger.info(f"✅ Mapped to ClickHouse schema: {len(final_df)} records")
    return final_df


def infer_market_from_existing_data(stock_master: ClickHouseStockMaster, df: pl.DataFrame) -> pl.DataFrame:
    """기존 ClickHouse 데이터에서 시장 정보 추론"""
    logger.info("Inferring market information from existing data...")

    try:
        # 기존 상장 종목에서 시장 정보 가져오기
        existing_df = stock_master.get_active_stocks()

        if existing_df.is_empty():
            logger.warning("No existing active stocks found for market inference")
            return df.with_columns(pl.lit("UNKNOWN").alias('market'))

        # 종목코드로 조인하여 시장 정보 가져오기
        market_lookup = existing_df.select(['symbol', 'market']).unique(subset=['symbol'])

        # Left join으로 시장 정보 추가
        enhanced_df = df.join(market_lookup, on='symbol', how='left', suffix='_existing')

        # 시장 정보가 있으면 사용, 없으면 기본값
        enhanced_df = enhanced_df.with_columns(
            pl.when(pl.col('market_existing').is_not_null())
            .then(pl.col('market_existing'))
            .otherwise(pl.lit("UNKNOWN"))
            .alias('market')
        ).drop('market_existing')

        market_stats = enhanced_df.group_by('market').agg(pl.count().alias('count'))
        logger.info("Market distribution after inference:")
        for row in market_stats.iter_rows(named=True):
            logger.info(f"  {row['market']}: {row['count']} stocks")

        return enhanced_df

    except Exception as e:
        logger.error(f"Failed to infer market information: {e}")
        return df.with_columns(pl.lit("UNKNOWN").alias('market'))


def insert_delisted_stocks(stock_master: ClickHouseStockMaster, df: pl.DataFrame) -> int:
    """상장폐지 종목을 ClickHouse에 삽입"""
    logger.info(f"Inserting {len(df)} delisted stocks to ClickHouse...")

    if df.is_empty():
        logger.warning("No data to insert")
        return 0

    try:
        # 기존 종목과 중복 체크
        existing_symbols = set()
        try:
            all_existing = stock_master.get_active_stocks()
            delisted_existing = stock_master.get_delisted_stocks()

            if not all_existing.is_empty():
                existing_symbols.update(all_existing['symbol'].to_list())
            if not delisted_existing.is_empty():
                existing_symbols.update(delisted_existing['symbol'].to_list())

            logger.info(f"Found {len(existing_symbols)} existing symbols in ClickHouse")
        except Exception as e:
            logger.warning(f"Could not check existing symbols: {e}")

        # 새로운 종목만 필터링
        if existing_symbols:
            new_df = df.filter(~pl.col('symbol').is_in(list(existing_symbols)))
            logger.info(f"Filtered to {len(new_df)} new delisted stocks")
        else:
            new_df = df

        if new_df.is_empty():
            logger.info("No new delisted stocks to insert")
            return 0

        # 배치 삽입
        batch_size = 500
        total_inserted = 0

        for i in range(0, len(new_df), batch_size):
            batch_df = new_df.slice(i, batch_size)

            try:
                inserted_count = stock_master.insert_stocks(batch_df)
                total_inserted += inserted_count
                logger.info(f"Batch {i//batch_size + 1}: Inserted {inserted_count} stocks")
            except Exception as e:
                logger.error(f"Failed to insert batch {i//batch_size + 1}: {e}")

        return total_inserted

    except Exception as e:
        logger.error(f"Failed to insert delisted stocks: {e}")
        return 0


def main():
    """메인 실행 함수"""
    logger.info("Starting delisted stocks update from parquet...")

    try:
        # ClickHouse 연결
        stock_master = ClickHouseStockMaster()
        logger.info("ClickHouse connection established")

        # Parquet 파일 경로
        parquet_path = project_root / "data" / "raw" / "delisted_stocks_processed_20250917.parquet"

        # 1. Parquet 파일에서 데이터 로드
        df = load_delisted_from_parquet(parquet_path)
        if df.is_empty():
            logger.error("No data loaded from parquet file")
            return False

        # 2. 컬럼 매핑
        mapped_df = map_delisted_columns(df)

        # 3. 시장 정보 추론
        enhanced_df = infer_market_from_existing_data(stock_master, mapped_df)

        # 4. ClickHouse에 삽입
        inserted_count = insert_delisted_stocks(stock_master, enhanced_df)

        if inserted_count > 0:
            # 5. 테이블 최적화
            logger.info("Optimizing table...")
            stock_master.optimize_table()

            # 6. 최종 통계
            stats = stock_master.get_stock_count()
            logger.info("📊 Final statistics after update:")
            for market, counts in stats.items():
                logger.info(f"  {market}: {counts['active']} active, {counts['delisted']} delisted, {counts['total']} total")

            logger.info(f"✅ Successfully updated {inserted_count} delisted stocks")
        else:
            logger.info("No new stocks were inserted")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to update delisted stocks: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)