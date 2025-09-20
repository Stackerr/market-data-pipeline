#!/usr/bin/env python3
"""
신규 상장 종목 동기화 시스템

목적:
- KRX에서 2000년 이후 모든 신규 상장 이력 크롤링하여 상장일 정보 확보
- 상장일 데이터 정규화 및 검증
- ClickHouse에 상장일 정보 업데이트
- 크롤링 실패 종목 리포트 생성

실행: uv run python scripts/sync_new_listings.py
"""

import sys
import logging
import polars as pl
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.crawlers.krx_new_listing_crawler import KRXNewListingCrawler
from src.clickhouse.client import ClickHouseClient
from src.clickhouse.stock_master import ClickHouseStockMaster

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NewListingsSyncSystem:
    """신규 상장 종목 동기화 시스템"""

    def __init__(self):
        """초기화"""
        self.crawler = KRXNewListingCrawler()
        self.clickhouse_client = ClickHouseClient()
        self.stock_master = ClickHouseStockMaster()
        self.data_dir = project_root / "data" / "daily_batch"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        logger.info("🚀 NewListingsSyncSystem 초기화 완료")

    def sync_new_listings_full(self, start_year: int = 2000, skip_existing: bool = True) -> dict:
        """
        전체 신규 상장 이력 동기화

        Args:
            start_year: 시작 연도 (기본: 2000)
            skip_existing: 기존 데이터 있으면 스킵 여부

        Returns:
            dict: 동기화 결과 통계
        """
        logger.info(f"📅 신규 상장 이력 전체 동기화 시작 ({start_year}년 이후)")

        start_time = datetime.now()

        try:
            # 1. 전체 신규 상장 이력 크롤링
            logger.info("🕷️ KRX 신규 상장 이력 크롤링 중...")
            listings_df = self.crawler.crawl_all_listings_full_sync(start_year=start_year)

            if listings_df.is_empty():
                logger.warning("⚠️ 크롤링된 신규 상장 이력이 없습니다")
                return self._create_empty_stats()

            logger.info(f"✅ {len(listings_df)}개 신규 상장 이력 크롤링 완료")

            # 2. 백업 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.data_dir / f"sync_new_listings_{start_year}_{timestamp}.parquet"
            listings_df.write_parquet(backup_path)
            logger.info(f"💾 백업 저장: {backup_path}")

            # 3. 중복 확인 및 스킵 로직
            if skip_existing:
                stats = self._process_with_skip_logic(listings_df)
            else:
                stats = self._process_all_data(listings_df)

            # 4. 최종 결과
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info("📊 신규 상장 동기화 완료")
            logger.info(f"  ⏱️ 소요 시간: {duration:.1f}초")
            logger.info(f"  🔍 크롤링된 이력: {len(listings_df)}개")
            logger.info(f"  ✅ 처리된 종목: {stats['processed']}개")
            logger.info(f"  🆕 새로 추가된 종목: {stats['added']}개")
            logger.info(f"  ⏭️ 스킵된 종목: {stats['skipped']}개")
            logger.info(f"  ❌ 에러 발생: {stats['errors']}개")

            return stats

        except Exception as e:
            logger.error(f"❌ 신규 상장 동기화 실패: {e}")
            raise

    def _process_with_skip_logic(self, listings_df: pl.DataFrame) -> dict:
        """스킵 로직을 적용하여 데이터 처리"""
        logger.info("🔄 스킵 로직 적용하여 처리 중...")

        try:
            # 기존 종목 조회
            existing_symbols = set(self.stock_master.get_all_symbols())
            logger.info(f"📋 기존 종목 수: {len(existing_symbols)}개")

            # 새로운 종목만 필터링
            new_symbols = []
            for row in listings_df.rows(named=True):
                if row['symbol'] not in existing_symbols:
                    new_symbols.append(row['symbol'])

            if not new_symbols:
                logger.info("ℹ️ 새로 추가할 종목이 없습니다 (모든 종목이 이미 존재)")
                return {
                    'processed': len(listings_df),
                    'added': 0,
                    'skipped': len(listings_df),
                    'errors': 0
                }

            # 새로운 종목만 필터링한 DataFrame
            new_listings_df = listings_df.filter(pl.col('symbol').is_in(new_symbols))
            logger.info(f"🆕 새로 추가할 종목: {len(new_listings_df)}개")

            # ClickHouse에 상장일 정보 업데이트
            stats = self.stock_master.process_new_listings(new_listings_df)

            return {
                'processed': len(listings_df),
                'added': stats.get('added', 0),
                'skipped': len(listings_df) - len(new_listings_df),
                'errors': stats.get('errors', 0)
            }

        except Exception as e:
            logger.error(f"❌ 스킵 로직 처리 실패: {e}")
            raise

    def _process_all_data(self, listings_df: pl.DataFrame) -> dict:
        """모든 데이터 처리 (스킵 없음)"""
        logger.info("🔄 모든 데이터 처리 중...")

        try:
            # ClickHouse에 상장일 정보 업데이트
            stats = self.stock_master.process_new_listings(listings_df)

            return {
                'processed': len(listings_df),
                'added': stats.get('added', 0),
                'skipped': stats.get('skipped', 0),
                'errors': stats.get('errors', 0)
            }

        except Exception as e:
            logger.error(f"❌ 전체 데이터 처리 실패: {e}")
            raise

    def _create_empty_stats(self) -> dict:
        """빈 통계 생성"""
        return {
            'processed': 0,
            'added': 0,
            'skipped': 0,
            'errors': 0
        }

    def get_sync_status(self) -> dict:
        """동기화 상태 확인"""
        try:
            # ClickHouse 연결 확인
            self.clickhouse_client.ping()

            # 종목 수 확인
            stats = self.stock_master.get_stock_count()

            return {
                'status': 'healthy',
                'clickhouse_connected': True,
                'stock_counts': stats,
                'last_check': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'status': 'error',
                'clickhouse_connected': False,
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }


def main():
    """메인 함수"""
    import argparse

    parser = argparse.ArgumentParser(description="신규 상장 종목 동기화 시스템")
    parser.add_argument(
        '--start-year',
        type=int,
        default=2000,
        help='시작 연도 (기본: 2000)'
    )
    parser.add_argument(
        '--no-skip',
        action='store_true',
        help='기존 데이터도 다시 처리 (skip 로직 비활성화)'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='동기화 상태만 확인'
    )

    args = parser.parse_args()

    try:
        sync_system = NewListingsSyncSystem()

        if args.status:
            status = sync_system.get_sync_status()
            logger.info(f"📊 동기화 상태: {status}")
            return True

        # 신규 상장 동기화 실행
        skip_existing = not args.no_skip
        stats = sync_system.sync_new_listings_full(
            start_year=args.start_year,
            skip_existing=skip_existing
        )

        # 성공 여부 판단
        success = stats['errors'] == 0

        if success:
            logger.info("✅ 신규 상장 동기화 성공적으로 완료!")
        else:
            logger.error("❌ 신규 상장 동기화 중 오류 발생")

        return success

    except Exception as e:
        logger.error(f"❌ 스크립트 실행 실패: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)