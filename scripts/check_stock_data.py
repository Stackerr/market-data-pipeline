#!/usr/bin/env python3
"""
ClickHouse 주식 마스터 데이터 현황 확인 스크립트
"""

import sys
import logging
from pathlib import Path

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.clickhouse.stock_master import ClickHouseStockMaster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_stock_data():
    """ClickHouse 주식 마스터 데이터 현황 확인"""
    try:
        stock_master = ClickHouseStockMaster()

        print("="*60)
        print("📊 CLICKHOUSE STOCK MASTER 데이터 현황")
        print("="*60)

        # 전체 종목 수 및 시장별 현황
        stats = stock_master.get_stock_count()

        total_active = 0
        total_delisted = 0

        for market, counts in stats.items():
            active = counts['active']
            delisted = counts['delisted']
            total = counts['total']

            total_active += active
            total_delisted += delisted

            print(f"📈 {market:>6}: {active:>5}개 활성, {delisted:>5}개 상장폐지, {total:>5}개 총합")

        print("-" * 60)
        print(f"📊 총합계: {total_active:>5}개 활성, {total_delisted:>5}개 상장폐지, {total_active + total_delisted:>5}개 전체")

        # 최신 데이터 샘플 확인
        print("\n📊 데이터 샘플 확인")
        print("-" * 40)

        # 활성 종목 샘플
        active_samples = stock_master.get_active_stocks(limit=5)
        if not active_samples.is_empty():
            print("✅ 활성 종목 샘플 (5개):")
            for row in active_samples.rows(named=True):
                listing_info = f" (상장: {row['listing_date']})" if row['listing_date'] else ""
                print(f"  {row['symbol']} - {row['name']} [{row['market']}]{listing_info}")

        # 상장폐지 종목 샘플
        delisted_samples = stock_master.get_delisted_stocks(limit=5)
        if not delisted_samples.is_empty():
            print("\n❌ 상장폐지 종목 샘플 (5개):")
            for row in delisted_samples.rows(named=True):
                delisting_info = f" (폐지: {row['delisting_date']})" if row['delisting_date'] else ""
                print(f"  {row['symbol']} - {row['name']} [{row['market']}]{delisting_info}")

        print("\n" + "="*60)

    except Exception as e:
        logger.error(f"Error checking stock data: {e}")
        raise

def check_data_quality():
    """데이터 품질 확인"""
    try:
        stock_master = ClickHouseStockMaster()

        print("\n🔍 데이터 품질 현황")
        print("="*60)

        # 누락 데이터 확인
        total_stocks = stock_master.get_total_count()

        # 상장일 누락 확인 (활성 종목)
        active_no_listing = stock_master.get_stocks_missing_listing_date()
        print(f"❗ 활성 종목 중 상장일 누락: {len(active_no_listing)}개")

        # 상장폐지일 누락 확인 (상장폐지 종목)
        delisted_no_date = stock_master.get_delisted_stocks_missing_date()
        print(f"❗ 상장폐지 종목 중 폐지일 누락: {len(delisted_no_date)}개")

        # 데이터 품질 점수
        quality_score = ((total_stocks - len(active_no_listing) - len(delisted_no_date)) / total_stocks) * 100
        print(f"📊 데이터 품질 점수: {quality_score:.1f}%")

    except Exception as e:
        logger.error(f"Error checking data quality: {e}")
        # 일부 메서드가 없을 수 있으므로 계속 진행
        pass

if __name__ == "__main__":
    check_stock_data()
    check_data_quality()