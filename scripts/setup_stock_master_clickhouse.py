#!/usr/bin/env python3
"""
ClickHouse stock_master 테이블 생성 스크립트
"""

import logging
import sys
from pathlib import Path

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.clickhouse.stock_master import ClickHouseStockMaster

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """메인 실행 함수"""
    logger.info("Starting ClickHouse stock_master table setup...")

    try:
        # ClickHouse 연결 테스트
        stock_master = ClickHouseStockMaster()
        logger.info("ClickHouse connection established successfully")

        # 테이블 생성
        if stock_master.create_table():
            logger.info("✅ stock_master table created successfully")

            # 테이블 상태 확인
            stats = stock_master.get_stock_count()
            logger.info(f"📊 Current table stats: {stats}")

        else:
            logger.error("❌ Failed to create stock_master table")
            return False

    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        return False

    logger.info("✅ ClickHouse stock_master setup completed successfully!")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)