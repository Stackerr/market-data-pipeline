import subprocess
import polars as pl
import json
from pathlib import Path
from datetime import date, datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickHouseClient:
    """ClickHouse 클라이언트 래퍼"""

    def __init__(self, host='localhost', port=9000, database='market_data'):
        self.host = host
        self.port = port
        self.database = database

    def execute_query(self, query: str, format='TabSeparatedWithNames'):
        """쿼리 실행 및 결과 반환"""
        cmd = [
            'clickhouse-client',
            '--host', self.host,
            '--port', str(self.port),
            '--database', self.database,
            '--query', query,
            '--format', format
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout

        except subprocess.CalledProcessError as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"stderr: {e.stderr}")
            raise

    def query_to_polars(self, query: str) -> pl.DataFrame:
        """쿼리 결과를 Polars DataFrame으로 반환"""
        try:
            # TSV 형식으로 결과 가져오기
            result = self.execute_query(query, format='TSVWithNames')

            if not result.strip():
                return pl.DataFrame()

            # 임시 파일에 저장 후 읽기
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
                f.write(result)
                temp_path = f.name

            try:
                df = pl.read_csv(temp_path, separator='\t')
                return df
            finally:
                Path(temp_path).unlink()

        except Exception as e:
            logger.error(f"Failed to convert to Polars: {e}")
            raise

    def get_stock_data(self, symbols: list, start_date: date = None,
                      end_date: date = None) -> pl.DataFrame:
        """종목 가격 데이터 조회 (최적화된 쿼리)"""

        # 기본 쿼리
        query = f"""
        SELECT
            sp.symbol,
            sm.name,
            sp.trade_date,
            sp.open_price,
            sp.high_price,
            sp.low_price,
            sp.close_price,
            sp.volume,
            sp.amount
        FROM stock_price sp
        INNER JOIN stock_master sm ON sp.symbol = sm.symbol
        WHERE sp.symbol IN ({','.join([f"'{s}'" for s in symbols])})
        """

        # 날짜 필터 추가
        if start_date:
            query += f" AND sp.trade_date >= '{start_date}'"
        if end_date:
            query += f" AND sp.trade_date <= '{end_date}'"

        query += " ORDER BY sp.symbol, sp.trade_date"

        return self.query_to_polars(query)

    def get_market_summary(self, trade_date: date) -> pl.DataFrame:
        """특정 날짜 시장 요약"""
        query = f"""
        SELECT
            market,
            count() as stock_count,
            avg(close_price) as avg_price,
            sum(volume) as total_volume,
            sum(amount) as total_amount
        FROM stock_price sp
        INNER JOIN stock_master sm ON sp.symbol = sm.symbol
        WHERE sp.trade_date = '{trade_date}'
          AND sp.close_price > 0
        GROUP BY market
        ORDER BY total_amount DESC
        """

        return self.query_to_polars(query)

    def get_top_performers(self, trade_date: date, limit: int = 20) -> pl.DataFrame:
        """특정 날짜 상위 수익률 종목"""
        query = f"""
        WITH price_changes AS (
            SELECT
                sp.symbol,
                sm.name,
                sp.close_price,
                lagInFrame(sp.close_price) OVER (PARTITION BY sp.symbol ORDER BY sp.trade_date) as prev_close,
                sp.volume
            FROM stock_price sp
            INNER JOIN stock_master sm ON sp.symbol = sm.symbol
            WHERE sp.trade_date = '{trade_date}'
        )
        SELECT
            symbol,
            name,
            close_price,
            prev_close,
            (close_price - prev_close) / prev_close * 100 as change_pct,
            volume
        FROM price_changes
        WHERE prev_close > 0
        ORDER BY change_pct DESC
        LIMIT {limit}
        """

        return self.query_to_polars(query)

    def get_backtest_universe(self, start_date: date, end_date: date,
                             min_market_cap: int = None) -> list:
        """백테스트 유니버스 생성"""
        query = f"""
        SELECT DISTINCT symbol
        FROM stock_master sm
        WHERE sm.is_active = 1
          AND (sm.listing_date IS NULL OR sm.listing_date <= '{start_date}')
          AND (sm.delisting_date IS NULL OR sm.delisting_date >= '{end_date}')
        """

        if min_market_cap:
            # 시가총액 조건 추가 (approximation)
            query += f"""
            AND symbol IN (
                SELECT symbol
                FROM stock_price
                WHERE trade_date = '{start_date}'
                  AND close_price * volume > {min_market_cap}
            )
            """

        query += " ORDER BY symbol"

        result = self.query_to_polars(query)
        return result['symbol'].to_list() if len(result) > 0 else []

    def calculate_returns(self, symbols: list, start_date: date,
                         end_date: date) -> pl.DataFrame:
        """수익률 계산 (벡터화 연산 활용)"""
        query = f"""
        SELECT
            symbol,
            trade_date,
            close_price,
            close_price / lagInFrame(close_price, 1) OVER (
                PARTITION BY symbol ORDER BY trade_date
            ) - 1 as daily_return,

            close_price / first_value(close_price) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) - 1 as cumulative_return

        FROM stock_price
        WHERE symbol IN ({','.join([f"'{s}'" for s in symbols])})
          AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY symbol, trade_date
        """

        return self.query_to_polars(query)

    def get_correlation_matrix(self, symbols: list, start_date: date,
                              end_date: date) -> pl.DataFrame:
        """종목간 상관관계 매트릭스 (ClickHouse 집계 함수 활용)"""
        if len(symbols) > 50:
            logger.warning("Too many symbols for correlation matrix, limiting to first 50")
            symbols = symbols[:50]

        # 피벗 테이블 형태로 데이터 재구성
        query = f"""
        WITH daily_returns AS (
            SELECT
                symbol,
                trade_date,
                close_price / lagInFrame(close_price, 1) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) - 1 as return
            FROM stock_price
            WHERE symbol IN ({','.join([f"'{s}'" for s in symbols])})
              AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        )
        SELECT
            a.symbol as symbol_a,
            b.symbol as symbol_b,
            corr(a.return, b.return) as correlation
        FROM daily_returns a
        INNER JOIN daily_returns b ON a.trade_date = b.trade_date
        WHERE a.return IS NOT NULL AND b.return IS NOT NULL
        GROUP BY a.symbol, b.symbol
        ORDER BY a.symbol, b.symbol
        """

        return self.query_to_polars(query)

    def optimize_tables(self):
        """테이블 최적화 실행"""
        logger.info("Optimizing ClickHouse tables...")

        optimize_queries = [
            "OPTIMIZE TABLE stock_master FINAL",
            "OPTIMIZE TABLE stock_price FINAL"
        ]

        for query in optimize_queries:
            try:
                self.execute_query(query)
                logger.info(f"Optimized: {query}")
            except Exception as e:
                logger.warning(f"Optimization failed for {query}: {e}")

# 사용 예제
def example_usage():
    """ClickHouse 클라이언트 사용 예제"""
    client = ClickHouseClient()

    # 1. 종목 데이터 조회
    symbols = ['005930', '000660', '035420']  # 삼성전자, SK하이닉스, NAVER
    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)

    logger.info("=== 예제 1: 종목 데이터 조회 ===")
    stock_data = client.get_stock_data(symbols, start_date, end_date)
    print(f"Stock data shape: {stock_data.shape}")
    print(stock_data.head())

    # 2. 시장 요약
    logger.info("=== 예제 2: 시장 요약 ===")
    market_summary = client.get_market_summary(date(2023, 12, 29))
    print(market_summary)

    # 3. 상위 수익률 종목
    logger.info("=== 예제 3: 상위 수익률 종목 ===")
    top_performers = client.get_top_performers(date(2023, 12, 29))
    print(top_performers.head())

    # 4. 백테스트 유니버스
    logger.info("=== 예제 4: 백테스트 유니버스 ===")
    universe = client.get_backtest_universe(
        start_date=date(2020, 1, 1),
        end_date=date(2023, 12, 31)
    )
    print(f"Universe size: {len(universe)}")

    # 5. 수익률 계산
    logger.info("=== 예제 5: 수익률 계산 ===")
    returns = client.calculate_returns(symbols[:3], start_date, end_date)
    print(f"Returns data shape: {returns.shape}")
    print(returns.head())

if __name__ == "__main__":
    example_usage()