import polars as pl
from src.database.connection import db_connection
from src.storage.models import StockMaster
from src.storage.stock_price import StockPrice
from pathlib import Path
import logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParquetExporter:
    def __init__(self, output_dir="/Users/user/Documents/GitHub/market-data-pipeline/data/parquet"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_stock_master(self):
        """종목 마스터 데이터를 Parquet으로 내보내기"""
        logger.info("Exporting stock master data...")

        session = db_connection.get_session()
        try:
            # StockMaster 데이터 조회
            query = session.query(
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

            # Polars DataFrame으로 변환
            data = []
            for row in query:
                data.append({
                    'symbol': row.symbol,
                    'name': row.name,
                    'market': row.market,
                    'sector': row.sector or '',
                    'industry': row.industry or '',
                    'listing_date': row.listing_date,
                    'delisting_date': row.delisting_date,
                    'is_active': row.is_active,
                    'delisting_reason': row.delisting_reason or ''
                })

            df = pl.DataFrame(data)

            # Parquet으로 저장
            master_path = self.output_dir / "stock_master.parquet"
            df.write_parquet(master_path)

            logger.info(f"Stock master exported: {len(df)} records -> {master_path}")
            return len(df)

        finally:
            session.close()

    def export_prices_by_symbol(self, batch_size=100):
        """종목별 가격 데이터를 개별 Parquet 파일로 내보내기"""
        logger.info("Exporting price data by symbol...")

        # 가격 데이터 디렉토리 생성
        prices_dir = self.output_dir / "prices"
        prices_dir.mkdir(exist_ok=True)

        session = db_connection.get_session()
        try:
            # 모든 종목 조회
            symbols = session.query(StockMaster.symbol, StockMaster.name, StockMaster.is_active).all()
            logger.info(f"Found {len(symbols)} symbols to export")

            exported_count = 0
            total_records = 0

            for i, (symbol, name, is_active) in enumerate(symbols, 1):
                try:
                    # 종목별 가격 데이터 조회
                    price_query = session.query(
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
                    ).filter(StockPrice.symbol == symbol).order_by(StockPrice.trade_date).all()

                    if not price_query:
                        logger.warning(f"No price data for {symbol} ({name})")
                        continue

                    # DataFrame으로 변환
                    data = []
                    for row in price_query:
                        data.append({
                            'trade_date': row.trade_date,
                            'open_price': float(row.open_price) if row.open_price else None,
                            'high_price': float(row.high_price) if row.high_price else None,
                            'low_price': float(row.low_price) if row.low_price else None,
                            'close_price': float(row.close_price) if row.close_price else None,
                            'volume': int(row.volume) if row.volume else None,
                            'amount': int(row.amount) if row.amount else None,
                            'change': float(row.change) if row.change else None,
                            'data_source': row.data_source or 'KRX'
                        })

                    df = pl.DataFrame(data)

                    # 종목별 파일로 저장
                    symbol_path = prices_dir / f"{symbol}.parquet"
                    df.write_parquet(symbol_path)

                    exported_count += 1
                    total_records += len(df)

                    if i % 50 == 0:
                        logger.info(f"Progress: {i}/{len(symbols)} symbols, {exported_count} exported, {total_records:,} total records")

                except Exception as e:
                    logger.error(f"Error exporting {symbol} ({name}): {e}")

            logger.info(f"Price data export completed: {exported_count} symbols, {total_records:,} total records")
            return exported_count, total_records

        finally:
            session.close()

    def create_combined_dataset(self):
        """백테스트용 통합 데이터셋 생성"""
        logger.info("Creating combined dataset for backtesting...")

        prices_dir = self.output_dir / "prices"
        if not prices_dir.exists():
            logger.error("Prices directory not found. Run export_prices_by_symbol first.")
            return

        # 모든 가격 파일 찾기
        price_files = list(prices_dir.glob("*.parquet"))
        logger.info(f"Found {len(price_files)} price files")

        # 종목 마스터 로드
        master_path = self.output_dir / "stock_master.parquet"
        if not master_path.exists():
            logger.error("Stock master file not found. Run export_stock_master first.")
            return

        master_df = pl.read_parquet(master_path)

        # 백테스트용 통합 데이터 생성 (배치 처리)
        batch_size = 500
        combined_dfs = []

        for i in range(0, len(price_files), batch_size):
            batch_files = price_files[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch_files)} files")

            batch_dfs = []
            for price_file in batch_files:
                symbol = price_file.stem
                try:
                    # 가격 데이터 로드
                    price_df = pl.read_parquet(price_file)

                    # 종목 코드 추가
                    price_df = price_df.with_columns(pl.lit(symbol).alias('symbol'))

                    batch_dfs.append(price_df)

                except Exception as e:
                    logger.warning(f"Error reading {price_file}: {e}")

            if batch_dfs:
                # 배치 결합
                batch_combined = pl.concat(batch_dfs, ignore_index=True)

                # 종목 마스터와 조인
                batch_combined = batch_combined.join(
                    master_df.select(['symbol', 'name', 'market', 'is_active', 'listing_date', 'delisting_date']),
                    on='symbol',
                    how='left'
                )

                combined_dfs.append(batch_combined)

        if combined_dfs:
            # 전체 결합
            logger.info("Combining all batches...")
            full_combined = pl.concat(combined_dfs, ignore_index=True)

            # 정렬 (날짜, 종목코드 순)
            full_combined = full_combined.sort(['trade_date', 'symbol'])

            # 백테스트용 파일로 저장
            combined_path = self.output_dir / "all_stocks_combined.parquet"
            full_combined.write_parquet(combined_path)

            logger.info(f"Combined dataset created: {len(full_combined):,} records -> {combined_path}")

            # 요약 정보 출력
            unique_symbols = full_combined['symbol'].n_unique()
            date_range = (full_combined['trade_date'].min(), full_combined['trade_date'].max())

            logger.info(f"Dataset summary: {unique_symbols} symbols, {date_range[0]} ~ {date_range[1]}")

    def create_yearly_partitions(self):
        """연도별 파티션 생성 (대용량 처리용)"""
        logger.info("Creating yearly partitions...")

        combined_path = self.output_dir / "all_stocks_combined.parquet"
        if not combined_path.exists():
            logger.error("Combined dataset not found. Run create_combined_dataset first.")
            return

        # 연도별 디렉토리 생성
        yearly_dir = self.output_dir / "yearly"
        yearly_dir.mkdir(exist_ok=True)

        # 데이터 로드 (lazy로 메모리 효율성)
        df = pl.scan_parquet(combined_path)

        # 연도 추출
        df = df.with_columns(
            pl.col('trade_date').dt.year().alias('year')
        )

        # 연도별로 분할 저장
        years = df.select('year').unique().collect()['year'].to_list()

        for year in sorted(years):
            year_df = df.filter(pl.col('year') == year).select(pl.exclude('year'))
            year_path = yearly_dir / f"stocks_{year}.parquet"

            year_df.sink_parquet(year_path)
            logger.info(f"Created yearly partition: {year} -> {year_path}")

def main():
    """메인 실행 함수"""
    exporter = ParquetExporter()

    logger.info("Starting Parquet export process...")

    # 1. 종목 마스터 내보내기
    master_count = exporter.export_stock_master()

    # 2. 종목별 가격 데이터 내보내기
    symbol_count, record_count = exporter.export_prices_by_symbol()

    # 3. 백테스트용 통합 데이터셋 생성
    exporter.create_combined_dataset()

    # 4. 연도별 파티션 생성
    exporter.create_yearly_partitions()

    logger.info("Parquet export completed!")
    logger.info(f"Summary: {master_count} symbols, {record_count:,} price records")

if __name__ == "__main__":
    main()