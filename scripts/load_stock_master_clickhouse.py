#!/usr/bin/env python3
"""
FinanceDataReader를 사용한 상장기업 데이터 ClickHouse 적재 스크립트
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import polars as pl
import FinanceDataReader as fdr

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.clickhouse.stock_master import ClickHouseStockMaster

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_listed_companies() -> pl.DataFrame:
    """상장기업 데이터 로드 (KOSPI, KOSDAQ, ETF)"""
    logger.info("Loading listed companies from FinanceDataReader...")

    all_stocks = []

    try:
        # KOSPI 상장기업
        logger.info("Fetching KOSPI stocks...")
        kospi_df = fdr.StockListing('KOSPI')
        kospi_pl = pl.from_pandas(kospi_df).with_columns(
            pl.lit('KOSPI').alias('market')
        )
        all_stocks.append(kospi_pl)
        logger.info(f"✅ KOSPI stocks loaded: {len(kospi_pl)}")

        # KOSDAQ 상장기업
        logger.info("Fetching KOSDAQ stocks...")
        kosdaq_df = fdr.StockListing('KOSDAQ')
        kosdaq_pl = pl.from_pandas(kosdaq_df).with_columns(
            pl.lit('KOSDAQ').alias('market')
        )
        all_stocks.append(kosdaq_pl)
        logger.info(f"✅ KOSDAQ stocks loaded: {len(kosdaq_pl)}")

        # KONEX 상장기업 (있는 경우)
        try:
            logger.info("Fetching KONEX stocks...")
            konex_df = fdr.StockListing('KONEX')
            konex_pl = pl.from_pandas(konex_df).with_columns(
                pl.lit('KONEX').alias('market')
            )
            all_stocks.append(konex_pl)
            logger.info(f"✅ KONEX stocks loaded: {len(konex_pl)}")
        except Exception as e:
            logger.warning(f"KONEX data not available: {e}")

        # ETF 데이터 (FinanceDataReader의 ETF 기능이 불안정하므로 건너뛰기)
        try:
            logger.info("Fetching ETF data...")
            etf_df = fdr.StockListing('ETF')
            etf_pl = pl.from_pandas(etf_df).with_columns(
                pl.lit('ETF').alias('market')
            )
            all_stocks.append(etf_pl)
            logger.info(f"✅ ETF data loaded: {len(etf_pl)}")
        except (IndexError, KeyError, ValueError) as e:
            logger.warning(f"ETF data collection failed due to FinanceDataReader library issue: {e}")
            logger.info("Skipping ETF data collection for now - this is a known issue with FinanceDataReader")
        except Exception as e:
            logger.warning(f"ETF data not available due to unexpected error: {e}")

    except Exception as e:
        logger.error(f"Failed to load stock listings: {e}")
        raise

    # 데이터 통합
    if not all_stocks:
        raise ValueError("No stock data loaded")

    combined_df = pl.concat(all_stocks)
    logger.info(f"📊 Total stocks loaded: {len(combined_df)}")

    return combined_df


def clean_and_prepare_data(stocks_df: pl.DataFrame) -> pl.DataFrame:
    """데이터 정리 및 변환"""
    logger.info("Cleaning and preparing stock data...")

    # 컬럼명 통일 (FinanceDataReader의 컬럼명에 따라 조정)
    column_mapping = {
        'Code': 'symbol',
        'Name': 'name',
        'Symbol': 'symbol'  # ETF의 경우 Symbol 사용
    }

    # 컬럼명 변경
    for old_col, new_col in column_mapping.items():
        if old_col in stocks_df.columns:
            stocks_df = stocks_df.rename({old_col: new_col})

    # 필수 컬럼 확인
    required_columns = ['symbol', 'name', 'market']
    for col in required_columns:
        if col not in stocks_df.columns:
            raise ValueError(f"Required column '{col}' not found in data")

    # 데이터 정리
    cleaned_df = stocks_df.select([
        pl.col('symbol').cast(pl.String).str.strip_chars(),
        pl.col('name').cast(pl.String).str.strip_chars(),
        pl.col('market').cast(pl.String),
        pl.lit(None).alias('listing_date').cast(pl.Date),  # FinanceDataReader는 상장일 제공 안함
        pl.lit(None).alias('delisting_date').cast(pl.Date),
        pl.lit(1).alias('is_active').cast(pl.UInt8)  # 상장기업은 모두 active
    ])

    # 6자리 숫자 종목코드만 필터링 (한국 주식)
    cleaned_df = cleaned_df.filter(
        (pl.col('symbol').str.len_chars() == 6) &
        (pl.col('symbol').str.contains(r'^\d{6}$'))
    )

    # 중복 제거 (symbol 기준)
    cleaned_df = cleaned_df.unique(subset=['symbol'])

    # 빈 이름 제거
    cleaned_df = cleaned_df.filter(
        pl.col('name').str.len_chars() > 0
    )

    logger.info(f"✅ Data cleaned: {len(cleaned_df)} valid stocks")

    return cleaned_df


def main():
    """메인 실행 함수"""
    logger.info("Starting listed companies data loading to ClickHouse...")

    try:
        # ClickHouse 연결
        stock_master = ClickHouseStockMaster()
        logger.info("ClickHouse connection established")

        # 테이블 생성 (존재하지 않는 경우)
        stock_master.create_table()

        # 1. 상장기업 데이터 로드
        stocks_df = load_listed_companies()

        # 2. 데이터 정리
        cleaned_df = clean_and_prepare_data(stocks_df)

        if cleaned_df.is_empty():
            logger.warning("No valid stock data to insert")
            return False

        # 3. ClickHouse에 데이터 삽입
        logger.info(f"Inserting {len(cleaned_df)} stocks into ClickHouse...")
        inserted_count = stock_master.insert_stocks(cleaned_df)

        if inserted_count > 0:
            logger.info(f"✅ Successfully inserted {inserted_count} stocks")

            # 4. 테이블 최적화
            logger.info("Optimizing table...")
            stock_master.optimize_table()

            # 5. 결과 통계
            stats = stock_master.get_stock_count()
            logger.info("📊 Final statistics:")
            for market, counts in stats.items():
                logger.info(f"  {market}: {counts['active']} active, {counts['delisted']} delisted, {counts['total']} total")

        else:
            logger.warning("No stocks were inserted")
            return False

    except Exception as e:
        logger.error(f"❌ Failed to load stock data: {e}")
        return False

    logger.info("✅ Stock master data loading completed successfully!")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)