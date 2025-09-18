import polars as pl
from pathlib import Path
from datetime import datetime, date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BacktestDataLoader:
    def __init__(self, data_dir="/Users/user/Documents/GitHub/market-data-pipeline/data/parquet"):
        self.data_dir = Path(data_dir)
        self.master_df = None
        self._load_master()

    def _load_master(self):
        """종목 마스터 데이터 로드"""
        master_path = self.data_dir / "stock_master.parquet"
        if master_path.exists():
            self.master_df = pl.read_parquet(master_path)
            logger.info(f"Loaded master data: {len(self.master_df)} stocks")
        else:
            logger.warning("Stock master file not found")

    def get_active_stocks_at_date(self, target_date: date) -> pl.DataFrame:
        """특정 날짜에 상장되어 있던 종목들 조회"""
        if self.master_df is None:
            return pl.DataFrame()

        # 상장일 <= target_date <= 상장폐지일 (또는 상장폐지일이 None)
        active_stocks = self.master_df.filter(
            (pl.col('listing_date') <= target_date) &
            ((pl.col('delisting_date') >= target_date) | (pl.col('delisting_date').is_null()))
        )

        logger.info(f"Active stocks at {target_date}: {len(active_stocks)}")
        return active_stocks

    def load_multiple_stocks_fast(self, symbols: list, start_date: date = None, end_date: date = None) -> pl.DataFrame:
        """여러 종목 데이터를 빠르게 로드 (개별 파일 방식)"""
        start_time = datetime.now()

        prices_dir = self.data_dir / "prices"
        if not prices_dir.exists():
            logger.error("Prices directory not found")
            return pl.DataFrame()

        dfs = []
        found_symbols = []

        for symbol in symbols:
            symbol_path = prices_dir / f"{symbol}.parquet"
            if symbol_path.exists():
                try:
                    # Lazy 스캔으로 메모리 효율성 증대
                    df = pl.scan_parquet(symbol_path)

                    # 날짜 필터링
                    if start_date:
                        df = df.filter(pl.col('trade_date') >= start_date)
                    if end_date:
                        df = df.filter(pl.col('trade_date') <= end_date)

                    # 종목 코드 추가
                    df = df.with_columns(pl.lit(symbol).alias('symbol'))

                    dfs.append(df)
                    found_symbols.append(symbol)

                except Exception as e:
                    logger.warning(f"Error loading {symbol}: {e}")

        if not dfs:
            return pl.DataFrame()

        # 모든 데이터를 한번에 결합하여 수집
        result = pl.concat(dfs).collect()

        # 종목 마스터와 조인하여 종목명 등 추가
        if self.master_df is not None:
            result = result.join(
                self.master_df.select(['symbol', 'name', 'market', 'is_active']),
                on='symbol',
                how='left'
            )

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Loaded {len(found_symbols)}/{len(symbols)} symbols, {len(result):,} records in {elapsed:.2f}s")

        return result.sort(['symbol', 'trade_date'])

    def load_yearly_data(self, year: int, symbols: list = None) -> pl.DataFrame:
        """연도별 파티션에서 데이터 로드"""
        yearly_dir = self.data_dir / "yearly"
        yearly_path = yearly_dir / f"stocks_{year}.parquet"

        if not yearly_path.exists():
            logger.error(f"Yearly data for {year} not found")
            return pl.DataFrame()

        start_time = datetime.now()

        # Lazy 스캔으로 시작
        df = pl.scan_parquet(yearly_path)

        # 종목 필터링
        if symbols:
            df = df.filter(pl.col('symbol').is_in(symbols))

        # 데이터 수집
        result = df.collect()

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Loaded {year} data: {len(result):,} records in {elapsed:.2f}s")

        return result

    def get_backtest_universe(self, start_date: date, end_date: date,
                             min_market_cap: int = None,
                             markets: list = None) -> list:
        """백테스트 유니버스 생성 (기간 내 지속적으로 상장된 종목들)"""
        if self.master_df is None:
            return []

        # 기본 필터: 기간 내 상장되어 있던 종목
        universe = self.master_df.filter(
            (pl.col('listing_date') <= start_date) &
            ((pl.col('delisting_date') >= end_date) | (pl.col('delisting_date').is_null()))
        )

        # 시장 필터링
        if markets:
            universe = universe.filter(pl.col('market').is_in(markets))

        # 상장 종목만 (폐지종목 제외)
        universe = universe.filter(pl.col('is_active') == True)

        symbols = universe['symbol'].to_list()
        logger.info(f"Backtest universe: {len(symbols)} stocks for {start_date} ~ {end_date}")

        return symbols

    def get_survivorship_bias_free_universe(self, start_date: date, end_date: date) -> list:
        """생존편향 없는 유니버스 (기간 내 상장폐지된 종목도 포함)"""
        if self.master_df is None:
            return []

        # 기간 내 언제라도 상장되어 있던 종목들 (생존편향 제거)
        universe = self.master_df.filter(
            (pl.col('listing_date') <= end_date) &
            ((pl.col('delisting_date') >= start_date) | (pl.col('delisting_date').is_null()))
        )

        symbols = universe['symbol'].to_list()
        logger.info(f"Survivorship-bias-free universe: {len(symbols)} stocks")

        return symbols

# 사용 예제들
def example_backtest_queries():
    """백테스트 쿼리 예제들"""
    loader = BacktestDataLoader()

    # 1. 2024년 초 상위 100개 종목의 2023년 데이터 로드
    logger.info("=== Example 1: Load 2023 data for top 100 stocks ===")

    # 임시로 삼성전자, SK하이닉스 등 몇개만 테스트
    test_symbols = ['005930', '000660', '035420', '005380', '051910']

    data_2023 = loader.load_multiple_stocks_fast(
        symbols=test_symbols,
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31)
    )
    print(f"2023 data shape: {data_2023.shape}")
    print(data_2023.head())

    # 2. 2020년 전체 데이터를 연도별 파티션에서 로드
    logger.info("=== Example 2: Load 2020 yearly data ===")

    data_2020 = loader.load_yearly_data(2020, symbols=test_symbols[:3])
    print(f"2020 data shape: {data_2020.shape}")

    # 3. 백테스트 유니버스 생성
    logger.info("=== Example 3: Create backtest universe ===")

    universe = loader.get_backtest_universe(
        start_date=date(2020, 1, 1),
        end_date=date(2023, 12, 31),
        markets=['KOSPI', 'KOSDAQ']
    )
    print(f"Universe size: {len(universe)}")
    print(f"Sample symbols: {universe[:10]}")

    # 4. 생존편향 없는 유니버스
    logger.info("=== Example 4: Survivorship-bias-free universe ===")

    full_universe = loader.get_survivorship_bias_free_universe(
        start_date=date(2020, 1, 1),
        end_date=date(2023, 12, 31)
    )
    print(f"Full universe size: {len(full_universe)}")

    # 5. 대용량 데이터 처리 예제 (1000개 종목)
    logger.info("=== Example 5: Large scale data loading ===")

    large_symbols = universe[:100] if len(universe) >= 100 else universe

    if large_symbols:
        large_data = loader.load_multiple_stocks_fast(
            symbols=large_symbols,
            start_date=date(2023, 1, 1),
            end_date=date(2023, 6, 30)  # 6개월만
        )
        print(f"Large data shape: {large_data.shape}")

        # 메모리 사용량 체크
        memory_mb = large_data.estimated_size() / (1024 * 1024)
        print(f"Estimated memory usage: {memory_mb:.1f} MB")

def performance_comparison():
    """성능 비교: PostgreSQL vs Parquet"""
    loader = BacktestDataLoader()

    symbols = ['005930', '000660', '035420', '005380', '051910']
    start_date = date(2020, 1, 1)
    end_date = date(2023, 12, 31)

    # Parquet 성능 측정
    start_time = datetime.now()
    parquet_data = loader.load_multiple_stocks_fast(symbols, start_date, end_date)
    parquet_time = (datetime.now() - start_time).total_seconds()

    print(f"Parquet loading: {len(parquet_data):,} records in {parquet_time:.2f}s")
    print(f"Performance: {len(parquet_data)/parquet_time:.0f} records/second")

if __name__ == "__main__":
    example_backtest_queries()
    performance_comparison()