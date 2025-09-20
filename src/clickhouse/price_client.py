#!/usr/bin/env python3
"""
ClickHouse 가격 데이터 전용 클라이언트 (Native Protocol)

대용량 가격 데이터 처리를 위한 고성능 클라이언트
- clickhouse-connect 라이브러리 사용 (Native Protocol)
- 배치 삽입 최적화
- 압축 지원
- 스트리밍 처리
"""

import clickhouse_connect
import polars as pl
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Union
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHousePriceClient:
    """가격 데이터 전용 ClickHouse 클라이언트 (Native Protocol)"""

    def __init__(self,
                 host='localhost',
                 port=8123,
                 username='default',
                 password='1q2w3e4r',
                 database='default'):
        """
        Native Protocol 기반 ClickHouse 클라이언트 초기화

        Args:
            host: ClickHouse 서버 호스트
            port: HTTP 프로토콜 포트 (기본 8123)
            username: 사용자명
            password: 비밀번호
            database: 데이터베이스명
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        # 클라이언트 연결
        self.client = None
        self._connect()

    def _connect(self):
        """ClickHouse 연결 설정"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                # 성능 최적화 설정
                connect_timeout=30,
                send_receive_timeout=300,
                compress=True,  # 압축 활성화
                # query_limit=1000000,  # 쿼리 제한 설정
            )

            # 연결 테스트
            result = self.client.query('SELECT 1')
            logger.info(f"✅ ClickHouse Native Protocol 연결 성공: {self.host}:{self.port}")

        except Exception as e:
            logger.error(f"❌ ClickHouse 연결 실패: {e}")
            raise

    def create_stock_price_table(self) -> bool:
        """stock_price 테이블 생성"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_price (
            symbol String,                          -- 종목코드
            trade_date Date,                       -- 거래일
            open_price Nullable(Float64),          -- 시가
            high_price Nullable(Float64),          -- 고가
            low_price Nullable(Float64),           -- 저가
            close_price Nullable(Float64),         -- 종가
            volume Nullable(UInt64),               -- 거래량
            amount Nullable(UInt64),               -- 거래대금
            change_rate Nullable(Float64),         -- 등락률
            market_cap Nullable(UInt64),           -- 시가총액
            create_dt DateTime DEFAULT now(),       -- 생성일시
            update_dt DateTime DEFAULT now()        -- 수정일시
        ) ENGINE = ReplacingMergeTree(update_dt)
        PARTITION BY toYYYYMM(trade_date)
        ORDER BY (symbol, trade_date)
        SETTINGS index_granularity = 8192
        """

        try:
            self.client.command(create_table_sql)
            logger.info("✅ stock_price 테이블 생성 완료")
            return True
        except Exception as e:
            logger.error(f"❌ stock_price 테이블 생성 실패: {e}")
            return False

    def drop_stock_price_table(self) -> bool:
        """stock_price 테이블 삭제 (테스트용)"""
        try:
            self.client.command("DROP TABLE IF EXISTS stock_price")
            logger.info("✅ stock_price 테이블 삭제 완료")
            return True
        except Exception as e:
            logger.error(f"❌ stock_price 테이블 삭제 실패: {e}")
            return False

    def insert_price_data_batch(self, price_df: pl.DataFrame, batch_size: int = 10000) -> int:
        """
        가격 데이터 배치 삽입 (대용량 처리 최적화)

        Args:
            price_df: 가격 데이터 DataFrame
            batch_size: 배치 크기

        Returns:
            삽입된 레코드 수
        """
        if price_df.is_empty():
            logger.warning("삽입할 데이터가 없습니다")
            return 0

        total_rows = len(price_df)
        inserted_count = 0

        logger.info(f"📈 가격 데이터 배치 삽입 시작: {total_rows:,} rows")
        start_time = time.time()

        try:
            # 배치 단위로 처리
            for i in range(0, total_rows, batch_size):
                batch_end = min(i + batch_size, total_rows)
                batch_df = price_df.slice(i, batch_end - i)

                # Polars DataFrame을 pandas로 변환 후 삽입
                batch_pandas = batch_df.to_pandas()

                # 디버깅: 첫 번째 배치의 데이터 구조 확인
                if i == 0:
                    logger.info(f"배치 컬럼: {list(batch_pandas.columns)}")
                    logger.info(f"배치 크기: {len(batch_pandas)}")

                # ClickHouse에 삽입 (DataFrame 직접 사용)
                self.client.insert_df('stock_price', batch_pandas)
                inserted_count += len(batch_pandas)

                # 진행률 로깅
                if i % (batch_size * 10) == 0:  # 10배치마다 로깅
                    progress = (i / total_rows) * 100
                    elapsed = time.time() - start_time
                    logger.info(f"진행률: {progress:.1f}% ({i:,}/{total_rows:,}) - 경과시간: {elapsed:.1f}s")

            elapsed_time = time.time() - start_time
            rate = inserted_count / elapsed_time if elapsed_time > 0 else 0

            logger.info(f"✅ 가격 데이터 삽입 완료: {inserted_count:,} rows in {elapsed_time:.2f}s ({rate:,.0f} rows/sec)")
            return inserted_count

        except Exception as e:
            logger.error(f"❌ 가격 데이터 삽입 실패: {e}")
            logger.error(f"오류 타입: {type(e)}")
            import traceback
            logger.error(f"상세 오류:\n{traceback.format_exc()}")
            return 0

    def get_price_data_count(self) -> int:
        """전체 가격 데이터 개수 조회"""
        try:
            result = self.client.query('SELECT count() as count FROM stock_price')
            count = result.first_row[0]
            logger.info(f"📊 총 가격 데이터: {count:,} rows")
            return count
        except Exception as e:
            logger.error(f"❌ 가격 데이터 개수 조회 실패: {e}")
            return 0

    def get_price_data_by_symbol(self, symbol: str, start_date: date = None, end_date: date = None) -> pl.DataFrame:
        """특정 종목의 가격 데이터 조회"""
        query = f"""
        SELECT
            symbol,
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            amount,
            change_rate,
            market_cap
        FROM stock_price
        WHERE symbol = '{symbol}'
        """

        if start_date:
            query += f" AND trade_date >= '{start_date}'"
        if end_date:
            query += f" AND trade_date <= '{end_date}'"

        query += " ORDER BY trade_date"

        try:
            result = self.client.query_df(query)
            # pandas DataFrame을 Polars로 변환
            if not result.empty:
                return pl.from_pandas(result)
            else:
                return pl.DataFrame()
        except Exception as e:
            logger.error(f"❌ 가격 데이터 조회 실패 [{symbol}]: {e}")
            return pl.DataFrame()

    def get_latest_trade_date(self, symbol: str = None) -> Optional[date]:
        """최신 거래일 조회"""
        if symbol:
            query = f"SELECT max(trade_date) as latest_date FROM stock_price WHERE symbol = '{symbol}'"
        else:
            query = "SELECT max(trade_date) as latest_date FROM stock_price"

        try:
            result = self.client.query(query)
            latest_date = result.first_row[0]
            return latest_date if latest_date else None
        except Exception as e:
            logger.error(f"❌ 최신 거래일 조회 실패: {e}")
            return None

    def get_symbols_with_data(self) -> List[str]:
        """가격 데이터가 있는 종목 목록 조회"""
        query = "SELECT DISTINCT symbol FROM stock_price ORDER BY symbol"

        try:
            result = self.client.query(query)
            symbols = [row[0] for row in result.result_rows]
            logger.info(f"📊 가격 데이터 보유 종목: {len(symbols)}개")
            return symbols
        except Exception as e:
            logger.error(f"❌ 종목 목록 조회 실패: {e}")
            return []

    def delete_price_data(self, symbol: str = None, start_date: date = None, end_date: date = None) -> bool:
        """가격 데이터 삭제"""
        conditions = []

        if symbol:
            conditions.append(f"symbol = '{symbol}'")
        if start_date:
            conditions.append(f"trade_date >= '{start_date}'")
        if end_date:
            conditions.append(f"trade_date <= '{end_date}'")

        if not conditions:
            logger.warning("삭제 조건이 없습니다. 전체 삭제를 방지합니다.")
            return False

        where_clause = " AND ".join(conditions)
        query = f"ALTER TABLE stock_price DELETE WHERE {where_clause}"

        try:
            self.client.command(query)
            logger.info(f"✅ 가격 데이터 삭제 완료: {where_clause}")
            return True
        except Exception as e:
            logger.error(f"❌ 가격 데이터 삭제 실패: {e}")
            return False

    def optimize_table(self) -> bool:
        """테이블 최적화 실행"""
        try:
            logger.info("🔧 stock_price 테이블 최적화 시작...")
            self.client.command("OPTIMIZE TABLE stock_price FINAL")
            logger.info("✅ stock_price 테이블 최적화 완료")
            return True
        except Exception as e:
            logger.error(f"❌ 테이블 최적화 실패: {e}")
            return False

    def get_table_info(self) -> Dict[str, Any]:
        """테이블 정보 및 통계 조회"""
        try:
            # 기본 통계
            count_result = self.client.query('SELECT count() as total_rows FROM stock_price')
            total_rows = count_result.first_row[0]

            # 날짜 범위
            date_result = self.client.query('''
                SELECT
                    min(trade_date) as earliest_date,
                    max(trade_date) as latest_date
                FROM stock_price
            ''')
            date_info = date_result.first_row

            # 종목 수
            symbol_result = self.client.query('SELECT countDistinct(symbol) as symbol_count FROM stock_price')
            symbol_count = symbol_result.first_row[0]

            # 파티션 정보
            partition_result = self.client.query('''
                SELECT
                    partition,
                    rows,
                    formatReadableSize(bytes_on_disk) as size
                FROM system.parts
                WHERE table = 'stock_price' AND active = 1
                ORDER BY partition
            ''')

            info = {
                'total_rows': total_rows,
                'symbol_count': symbol_count,
                'earliest_date': date_info[0] if date_info[0] else None,
                'latest_date': date_info[1] if date_info[1] else None,
                'partitions': [
                    {
                        'partition': row[0],
                        'rows': row[1],
                        'size': row[2]
                    } for row in partition_result.result_rows
                ]
            }

            logger.info("📊 테이블 정보:")
            logger.info(f"  총 레코드: {info['total_rows']:,}")
            logger.info(f"  종목 수: {info['symbol_count']:,}")
            logger.info(f"  날짜 범위: {info['earliest_date']} ~ {info['latest_date']}")
            logger.info(f"  파티션 수: {len(info['partitions'])}")

            return info

        except Exception as e:
            logger.error(f"❌ 테이블 정보 조회 실패: {e}")
            return {}

    def close(self):
        """연결 종료"""
        if self.client:
            self.client.close()
            logger.info("ClickHouse 연결 종료")


# 사용 예제
def example_usage():
    """가격 데이터 클라이언트 사용 예제"""

    # 클라이언트 생성
    price_client = ClickHousePriceClient()

    try:
        # 1. 테이블 생성
        price_client.create_stock_price_table()

        # 2. 샘플 데이터 생성 (테이블 스키마와 정확히 일치)
        sample_data = pl.DataFrame({
            'symbol': ['005930'] * 5,
            'trade_date': [date(2024, 1, i) for i in range(1, 6)],
            'open_price': [75000.0, 75100.0, 75200.0, 75300.0, 75400.0],
            'high_price': [75500.0, 75600.0, 75700.0, 75800.0, 75900.0],
            'low_price': [74500.0, 74600.0, 74700.0, 74800.0, 74900.0],
            'close_price': [75000.0, 75100.0, 75200.0, 75300.0, 75400.0],
            'volume': [1000000, 1100000, 1200000, 1300000, 1400000],
            'amount': [75000000000, 82610000000, 90240000000, 97890000000, 105560000000],
            'change_rate': [0.0, 0.13, 0.13, 0.13, 0.13],
            'market_cap': [450000000000000, 451500000000000, 453000000000000, 454500000000000, 456000000000000],
            'create_dt': [datetime.now()] * 5,
            'update_dt': [datetime.now()] * 5
        })

        # 3. 데이터 삽입
        inserted = price_client.insert_price_data_batch(sample_data)
        print(f"삽입된 레코드: {inserted}")

        # 4. 데이터 조회
        result = price_client.get_price_data_by_symbol('005930')
        print(f"조회된 데이터: {len(result)} rows")
        print(result)

        # 5. 테이블 정보
        price_client.get_table_info()

    finally:
        price_client.close()


if __name__ == "__main__":
    example_usage()