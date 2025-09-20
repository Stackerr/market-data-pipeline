#!/usr/bin/env python3
"""
일간 종목 마스터 데이터 업데이트 배치 스크립트
- 상장 종목 업데이트 (FinanceDataReader)
- 상장폐지 종목 크롤링 및 업데이트 (KRX)
- ClickHouse 데이터 동기화
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, date
from typing import Dict
import polars as pl

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.clickhouse.stock_master import ClickHouseStockMaster
from src.crawlers.krx_delisted_crawler import KRXDelistedCrawler
from src.crawlers.krx_new_listing_crawler import KRXNewListingCrawler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(project_root / 'logs' / f'daily_update_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DailyStockMasterUpdater:
    """일간 종목 마스터 데이터 업데이트 관리자"""

    def __init__(self):
        self.stock_master = ClickHouseStockMaster()
        self.krx_crawler = KRXDelistedCrawler()
        self.new_listing_crawler = KRXNewListingCrawler()
        self.data_dir = project_root / "data" / "daily_batch"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def update_listed_stocks(self) -> bool:
        """상장 종목 데이터 업데이트"""
        logger.info("🔄 Starting listed stocks update...")

        try:
            import FinanceDataReader as fdr

            all_stocks = []

            # KOSPI 상장기업
            logger.info("Fetching KOSPI stocks...")
            kospi_df = fdr.StockListing('KOSPI')
            kospi_pl = pl.from_pandas(kospi_df).with_columns(pl.lit('KOSPI').alias('market'))
            all_stocks.append(kospi_pl)
            logger.info(f"✅ KOSPI stocks loaded: {len(kospi_pl)}")

            # KOSDAQ 상장기업
            logger.info("Fetching KOSDAQ stocks...")
            kosdaq_df = fdr.StockListing('KOSDAQ')
            kosdaq_pl = pl.from_pandas(kosdaq_df).with_columns(pl.lit('KOSDAQ').alias('market'))
            all_stocks.append(kosdaq_pl)
            logger.info(f"✅ KOSDAQ stocks loaded: {len(kosdaq_pl)}")

            # KONEX 상장기업
            try:
                logger.info("Fetching KONEX stocks...")
                konex_df = fdr.StockListing('KONEX')
                konex_pl = pl.from_pandas(konex_df).with_columns(pl.lit('KONEX').alias('market'))
                all_stocks.append(konex_pl)
                logger.info(f"✅ KONEX stocks loaded: {len(konex_pl)}")
            except Exception as e:
                logger.warning(f"KONEX data not available: {e}")

            # 데이터 통합 및 정리
            combined_df = pl.concat(all_stocks)

            # 컬럼명 통일
            column_mapping = {'Code': 'symbol', 'Name': 'name'}
            for old_col, new_col in column_mapping.items():
                if old_col in combined_df.columns:
                    combined_df = combined_df.rename({old_col: new_col})

            # ClickHouse 스키마에 맞게 변환
            cleaned_df = combined_df.select([
                pl.col('symbol').cast(pl.String).str.strip_chars(),
                pl.col('name').cast(pl.String).str.strip_chars(),
                pl.col('market').cast(pl.String),
                pl.lit(None).alias('listing_date').cast(pl.Date),
                pl.lit(None).alias('delisting_date').cast(pl.Date),
                pl.lit(1).alias('is_active').cast(pl.UInt8)
            ])

            # 6자리 숫자 종목코드만 필터링
            cleaned_df = cleaned_df.filter(
                (pl.col('symbol').str.len_chars() == 6) &
                (pl.col('symbol').str.contains(r'^\d{6}$'))
            )

            # 중복 제거 및 빈 이름 제거
            cleaned_df = cleaned_df.unique(subset=['symbol']).filter(pl.col('name').str.len_chars() > 0)

            logger.info(f"📊 Processed {len(cleaned_df)} valid listed stocks")

            # ClickHouse에 업데이트 (UPSERT 방식)
            updated_count = self._upsert_stocks(cleaned_df, is_active=True)

            logger.info(f"✅ Listed stocks update completed: {updated_count} stocks processed")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to update listed stocks: {e}")
            return False

    def sync_all_delisted_stocks(self, start_year: int = 1990) -> bool:
        """전체 상장폐지 종목 데이터 동기화 (일간 배치용)"""
        logger.info(f"🔄 Syncing all delisted stocks from {start_year}...")

        try:
            # KRX에서 전체 상장폐지 데이터 크롤링
            delisted_df = self.krx_crawler.crawl_all_markets_full_sync(start_year=start_year)

            if delisted_df.is_empty():
                logger.info("ℹ️ No delisted data found in full sync")
                return True

            # Parquet으로 백업 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.data_dir / f"all_delisted_sync_{timestamp}.parquet"
            delisted_df.write_parquet(backup_path)
            logger.info(f"💾 All delisted data backed up to: {backup_path}")

            # ClickHouse 스키마에 맞게 변환
            processed_df = self._process_delisted_data(delisted_df)

            # ClickHouse에 업데이트 (기존에 없는 것만 추가)
            updated_count = self._upsert_stocks(processed_df, is_active=False)

            logger.info(f"📊 Full delisted sync results:")
            logger.info(f"  🔍 Total found: {len(delisted_df)}")
            logger.info(f"  ✅ Added: {updated_count}")

            return True

        except Exception as e:
            logger.error(f"❌ Failed to sync all delisted stocks: {e}")
            return False

    def update_delisted_stocks(self) -> bool:
        """상장폐지 종목 데이터 업데이트 (레거시 메서드)"""
        logger.warning("Using legacy update_delisted_stocks - consider using sync_all_delisted_stocks")
        logger.info("🔄 Starting delisted stocks update...")

        try:
            # KRX에서 상장폐지 데이터 크롤링
            delisted_df = self.krx_crawler.crawl_all_markets()

            if delisted_df.is_empty():
                logger.warning("No delisted data crawled from KRX")
                return False

            # Parquet으로 백업 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.data_dir / f"delisted_backup_{timestamp}.parquet"
            delisted_df.write_parquet(backup_path)
            logger.info(f"💾 Delisted data backed up to: {backup_path}")

            # ClickHouse 스키마에 맞게 변환
            processed_df = self._process_delisted_data(delisted_df)

            # ClickHouse에 업데이트
            updated_count = self._upsert_stocks(processed_df, is_active=False)

            logger.info(f"✅ Delisted stocks update completed: {updated_count} stocks processed")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to update delisted stocks: {e}")
            return False

    def sync_all_listings(self, start_year: int = 2020) -> bool:
        """전체 상장 데이터 동기화 (일간 배치용)"""
        logger.info(f"🔄 Syncing all listings from {start_year}...")

        try:
            # 전체 기간 신규상장 크롤링
            new_listings_df = self.new_listing_crawler.crawl_all_listings_full_sync(start_year=start_year)

            if new_listings_df.is_empty():
                logger.info("ℹ️ No listings found in full sync")
                return True

            # Parquet으로 백업 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.data_dir / f"all_listings_sync_{timestamp}.parquet"
            new_listings_df.write_parquet(backup_path)
            logger.info(f"💾 All listings data backed up to: {backup_path}")

            # 신규상장 처리 (기존에 없는 것만 추가)
            stats = self.stock_master.process_new_listings(new_listings_df)

            logger.info(f"📊 Full listings sync results:")
            logger.info(f"  🔍 Total found: {len(new_listings_df)}")
            logger.info(f"  ✅ Added: {stats['added']}")
            logger.info(f"  ⏭️ Skipped (existing): {stats['skipped']}")
            logger.info(f"  ❌ Errors: {stats['errors']}")

            # 에러가 없으면 성공
            return stats['errors'] == 0

        except Exception as e:
            logger.error(f"❌ Failed to sync all listings: {e}")
            return False

    def _process_delisted_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """상장폐지 데이터를 ClickHouse 스키마에 맞게 처리"""
        try:
            # 컬럼명 정규화
            column_mapping = {
                'company_code': 'symbol',
                'company_name': 'name',
                'delisting_date': 'delisting_date',
                'market': 'market'
            }

            processed_df = df
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    processed_df = processed_df.rename({old_col: new_col})

            # 필수 컬럼 추가 및 변환
            processed_df = processed_df.with_columns([
                pl.col('symbol').cast(pl.String).str.replace_all(r'[^\d]', '').str.slice(0, 6).alias('symbol'),
                pl.when(pl.col('name').is_null()).then(pl.lit("Unknown")).otherwise(pl.col('name')).alias('name'),
                pl.when(pl.col('market').is_null()).then(pl.lit("UNKNOWN")).otherwise(pl.col('market')).alias('market'),
                pl.lit(None).alias('listing_date').cast(pl.Date),
                pl.when(pl.col('delisting_date').is_null()).then(pl.date(2020, 1, 1)).otherwise(pl.col('delisting_date')).alias('delisting_date'),
                pl.lit(0).alias('is_active').cast(pl.UInt8)
            ])

            # 6자리 종목코드만 필터링
            processed_df = processed_df.filter(pl.col('symbol').str.len_chars() == 6)

            # 최종 스키마 선택
            final_df = processed_df.select([
                'symbol', 'name', 'market', 'listing_date', 'delisting_date', 'is_active'
            ])

            return final_df

        except Exception as e:
            logger.error(f"Failed to process delisted data: {e}")
            return pl.DataFrame()

    def _upsert_stocks(self, df: pl.DataFrame, is_active: bool = True) -> int:
        """UPSERT 방식으로 종목 데이터 업데이트"""
        if df.is_empty():
            return 0

        try:
            # 기존 데이터 조회
            existing_symbols = set()
            try:
                if is_active:
                    existing_df = self.stock_master.get_active_stocks()
                else:
                    existing_df = self.stock_master.get_delisted_stocks()

                if not existing_df.is_empty():
                    existing_symbols = set(existing_df['symbol'].to_list())

            except Exception as e:
                logger.warning(f"Could not retrieve existing data: {e}")

            # 새로운 데이터만 필터링
            if existing_symbols:
                new_df = df.filter(~pl.col('symbol').is_in(list(existing_symbols)))
                logger.info(f"Found {len(new_df)} new stocks out of {len(df)} total")
            else:
                new_df = df

            if new_df.is_empty():
                logger.info("No new stocks to insert")
                return 0

            # 배치 삽입
            return self.stock_master.insert_stocks(new_df)

        except Exception as e:
            logger.error(f"Failed to upsert stocks: {e}")
            return 0

    def optimize_and_report(self) -> Dict[str, int]:
        """테이블 최적화 및 최종 리포트"""
        logger.info("🔧 Optimizing ClickHouse table...")

        try:
            # 테이블 최적화
            self.stock_master.optimize_table()

            # 최종 통계
            stats = self.stock_master.get_stock_count()

            logger.info("📊 Final Database Statistics:")
            total_active = 0
            total_delisted = 0

            for market, counts in stats.items():
                active = counts['active']
                delisted = counts['delisted']
                total = counts['total']

                total_active += active
                total_delisted += delisted

                logger.info(f"  {market:>10}: {active:>5} active, {delisted:>5} delisted, {total:>5} total")

            logger.info(f"  {'TOTAL':>10}: {total_active:>5} active, {total_delisted:>5} delisted, {total_active + total_delisted:>5} total")

            return {
                'total_active': total_active,
                'total_delisted': total_delisted,
                'total_stocks': total_active + total_delisted
            }

        except Exception as e:
            logger.error(f"Failed to optimize and report: {e}")
            return {}

    def run_daily_update(self) -> bool:
        """일간 전체 업데이트 실행"""
        start_time = datetime.now()
        logger.info(f"🚀 Starting daily stock master update at {start_time}")

        try:
            # 1. 활성 상장 종목 업데이트 (FinanceDataReader)
            listed_success = self.update_listed_stocks()

            # 2. 전체 상장 이력 동기화 (KRX 크롤링)
            new_listing_success = self.sync_all_listings(start_year=2000)

            # 3. 전체 상장폐지 이력 동기화 (KRX 크롤링)
            delisted_success = self.sync_all_delisted_stocks(start_year=1990)

            # 4. 최적화 및 리포트
            final_stats = self.optimize_and_report()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # 최종 결과 (모든 동기화 작업이 성공해야 함)
            success = listed_success and new_listing_success and delisted_success

            if success:
                logger.info(f"✅ Daily update completed successfully in {duration:.1f} seconds")
                logger.info(f"📈 Final count: {final_stats.get('total_stocks', 0)} total stocks")
            else:
                logger.error(f"❌ Daily update failed after {duration:.1f} seconds")

            return success

        except Exception as e:
            logger.error(f"❌ Daily update crashed: {e}")
            return False


def main():
    """메인 실행 함수"""
    # 로그 디렉토리 생성
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)

    try:
        updater = DailyStockMasterUpdater()
        success = updater.run_daily_update()
        return success

    except Exception as e:
        logger.error(f"Failed to run daily update: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)