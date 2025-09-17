import pytest
import polars as pl
from datetime import date, datetime
from unittest.mock import Mock, patch
from src.clickhouse.stock_master import ClickHouseStockMaster


class TestClickHouseStockMaster:
    """ClickHouseStockMaster 클래스 테스트"""

    @pytest.fixture
    def mock_client(self):
        """Mock ClickHouse client"""
        return Mock()

    @pytest.fixture
    def stock_master(self, mock_client):
        """ClickHouseStockMaster 인스턴스 (mock client 사용)"""
        with patch('src.clickhouse.stock_master.get_clickhouse_client', return_value=mock_client):
            return ClickHouseStockMaster()

    @pytest.fixture
    def sample_stocks_df(self):
        """테스트용 샘플 종목 데이터"""
        return pl.DataFrame({
            'symbol': ['005930', '000660', '035420'],
            'name': ['삼성전자', 'SK하이닉스', 'NAVER'],
            'market': ['KOSPI', 'KOSPI', 'KOSPI'],
            'listing_date': [date(1975, 6, 11), date(1996, 12, 26), date(2002, 10, 29)],
            'delisting_date': [None, None, None],
            'is_active': [1, 1, 1]
        })

    def test_create_table_success(self, stock_master, mock_client):
        """테이블 생성 성공 테스트"""
        # Given
        mock_client.command.return_value = None

        # When
        result = stock_master.create_table()

        # Then
        assert result is True
        mock_client.command.assert_called_once()
        call_args = mock_client.command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS stock_master" in call_args
        assert "ENGINE = ReplacingMergeTree(update_dt)" in call_args

    def test_create_table_failure(self, stock_master, mock_client):
        """테이블 생성 실패 테스트"""
        # Given
        mock_client.command.side_effect = Exception("Connection failed")

        # When
        result = stock_master.create_table()

        # Then
        assert result is False

    def test_drop_table_success(self, stock_master, mock_client):
        """테이블 삭제 성공 테스트"""
        # Given
        mock_client.command.return_value = None

        # When
        result = stock_master.drop_table()

        # Then
        assert result is True
        mock_client.command.assert_called_once_with("DROP TABLE IF EXISTS stock_master")

    def test_insert_stocks_success(self, stock_master, mock_client, sample_stocks_df):
        """종목 데이터 삽입 성공 테스트"""
        # Given
        mock_client.insert.return_value = None

        # When
        result = stock_master.insert_stocks(sample_stocks_df)

        # Then
        assert result == 3
        mock_client.insert.assert_called_once()
        call_args = mock_client.insert.call_args
        assert call_args[0][0] == 'stock_master'  # 테이블명
        assert isinstance(call_args[0][1], dict)  # 데이터가 dict 형태

    def test_insert_stocks_empty_dataframe(self, stock_master, mock_client):
        """빈 DataFrame 삽입 테스트"""
        # Given
        empty_df = pl.DataFrame()

        # When
        result = stock_master.insert_stocks(empty_df)

        # Then
        assert result == 0
        mock_client.insert.assert_not_called()

    def test_insert_stocks_missing_columns(self, stock_master, mock_client):
        """필수 컬럼 누락 테스트"""
        # Given
        invalid_df = pl.DataFrame({'symbol': ['005930']})  # name, market 누락

        # When & Then
        with pytest.raises(ValueError) as exc_info:
            stock_master.insert_stocks(invalid_df)

        assert "Missing required columns" in str(exc_info.value)

    def test_update_delisting_date_success(self, stock_master, mock_client):
        """상장폐지일 업데이트 성공 테스트"""
        # Given
        symbol = "005930"
        delisting_date = date(2024, 12, 31)
        mock_client.command.return_value = None

        # When
        result = stock_master.update_delisting_date(symbol, delisting_date)

        # Then
        assert result is True
        mock_client.command.assert_called_once()
        call_args = mock_client.command.call_args[0][0]
        assert f"WHERE symbol = '{symbol}'" in call_args
        assert f"delisting_date = '{delisting_date}'" in call_args
        assert "is_active = 0" in call_args

    def test_get_stock_by_symbol_found(self, stock_master, mock_client):
        """종목 조회 성공 테스트"""
        # Given
        symbol = "005930"
        mock_result = Mock()
        mock_result.result_rows = [("005930", "삼성전자", "KOSPI", None, None, 1, datetime.now(), datetime.now())]
        mock_result.column_types = [("symbol", "String"), ("name", "String"), ("market", "String"),
                                  ("listing_date", "Date"), ("delisting_date", "Nullable(Date)"),
                                  ("is_active", "UInt8"), ("create_dt", "DateTime"), ("update_dt", "DateTime")]
        mock_client.query.return_value = mock_result

        # When
        result = stock_master.get_stock_by_symbol(symbol)

        # Then
        assert result is not None
        assert result['symbol'] == '005930'
        assert result['name'] == '삼성전자'

    def test_get_stock_by_symbol_not_found(self, stock_master, mock_client):
        """종목 조회 실패 테스트 (종목 없음)"""
        # Given
        symbol = "999999"
        mock_result = Mock()
        mock_result.result_rows = []
        mock_client.query.return_value = mock_result

        # When
        result = stock_master.get_stock_by_symbol(symbol)

        # Then
        assert result is None

    def test_get_active_stocks_success(self, stock_master, mock_client):
        """상장 종목 조회 성공 테스트"""
        # Given
        mock_result = pl.DataFrame({
            'symbol': ['005930', '000660'],
            'name': ['삼성전자', 'SK하이닉스'],
            'market': ['KOSPI', 'KOSPI'],
            'is_active': [1, 1]
        }).to_pandas()
        mock_client.query_df.return_value = mock_result

        # When
        result = stock_master.get_active_stocks()

        # Then
        assert not result.is_empty()
        assert len(result) == 2
        assert all(result['is_active'].to_list())

    def test_get_active_stocks_with_market_filter(self, stock_master, mock_client):
        """시장별 상장 종목 조회 테스트"""
        # Given
        market = "KOSDAQ"
        mock_result = pl.DataFrame().to_pandas()
        mock_client.query_df.return_value = mock_result

        # When
        result = stock_master.get_active_stocks(market=market)

        # Then
        call_args = mock_client.query_df.call_args[0][0]
        assert f"AND market = '{market}'" in call_args

    def test_get_stock_count_success(self, stock_master, mock_client):
        """종목 수 통계 성공 테스트"""
        # Given
        import pandas as pd
        mock_result = pd.DataFrame({
            'market': ['KOSPI', 'KOSDAQ'],
            'active_count': [800, 1200],
            'delisted_count': [100, 200],
            'total_count': [900, 1400]
        })
        mock_client.query_df.return_value = mock_result

        # When
        result = stock_master.get_stock_count()

        # Then
        assert 'KOSPI' in result
        assert 'KOSDAQ' in result
        assert result['KOSPI']['active'] == 800
        assert result['KOSPI']['delisted'] == 100
        assert result['KOSPI']['total'] == 900

    def test_optimize_table_success(self, stock_master, mock_client):
        """테이블 최적화 성공 테스트"""
        # Given
        mock_client.command.return_value = None

        # When
        result = stock_master.optimize_table()

        # Then
        assert result is True
        mock_client.command.assert_called_once_with("OPTIMIZE TABLE stock_master FINAL")