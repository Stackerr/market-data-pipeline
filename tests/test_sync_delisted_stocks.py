#!/usr/bin/env python3
"""
Phase 1.2: 상장폐지 종목 정보 수집 시스템 TDD 테스트

Phase 1.2 목적:
- KRX에서 1990년 이후 모든 상장폐지 이력 크롤링하여 상장폐지 정보 확보
- 상장폐지일 및 사유 정보 수집
- 중복 제거 및 데이터 품질 검증
- 상장폐지 종목 ClickHouse 적재

실행: uv run python -m pytest tests/test_phase_1_2_delisted_collection.py -v
"""

import pytest
import polars as pl
from datetime import datetime, date
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil

from src.clickhouse.client import ClickHouseClient
from src.crawlers.krx_delisted_crawler import KRXDelistedCrawler


class TestPhase1_2DelistedCollection:
    """Phase 1.2: 상장폐지 종목 정보 수집 시스템 테스트"""

    def setup_method(self):
        """각 테스트 메서드 실행 전 초기화"""
        self.temp_dir = tempfile.mkdtemp()
        self.crawler = KRXDelistedCrawler(data_dir=self.temp_dir)

    def teardown_method(self):
        """각 테스트 메서드 실행 후 정리"""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    # 1. KRX 상장폐지 정보 크롤러 기본 기능 테스트

    def test_krx_delisted_crawler_initialization(self):
        """크롤러 초기화 테스트"""
        crawler = KRXDelistedCrawler(data_dir=self.temp_dir)

        assert crawler.base_url == "https://kind.krx.co.kr/investwarn/delcompany.do"
        assert crawler.data_dir == Path(self.temp_dir)
        assert crawler.data_dir.exists()
        assert 'KOSPI' in crawler.market_codes
        assert 'KOSDAQ' in crawler.market_codes
        assert 'KONEX' in crawler.market_codes

    def test_search_form_data_generation(self):
        """검색 폼 데이터 생성 테스트"""
        form_data = self.crawler._get_search_form_data('Y', '20200101', '20231231')

        assert form_data['method'] == 'searchDelCompanyList'
        assert form_data['market'] == 'Y'
        assert form_data['fromDate'] == '20200101'
        assert form_data['toDate'] == '20231231'
        assert form_data['currentPageSize'] == '5000'

    def test_search_form_data_default_end_date(self):
        """기본 종료일 설정 테스트"""
        form_data = self.crawler._get_search_form_data('Y', '20200101')

        today = datetime.now().strftime("%Y%m%d")
        assert form_data['toDate'] == today

    # 2. HTML 파싱 및 데이터 처리 테스트

    def test_html_parsing_valid_table(self):
        """유효한 HTML 테이블 파싱 테스트"""
        # 모의 HTML 생성
        html_content = """
        <html>
        <body>
        <table>
            <tr>
                <th>순번</th>
                <th>회사명</th>
                <th>종목코드</th>
                <th>상장폐지일</th>
                <th>상장폐지사유</th>
            </tr>
            <tr>
                <td>1</td>
                <td>테스트회사</td>
                <td>123456</td>
                <td>2023.12.31</td>
                <td>테스트사유</td>
            </tr>
            <tr>
                <td>2</td>
                <td>다른회사</td>
                <td>654321</td>
                <td>2023.11.30</td>
                <td>다른사유</td>
            </tr>
        </table>
        </body>
        </html>
        """

        # HTML 파일 생성
        html_file = Path(self.temp_dir) / "test.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        # 파싱 실행
        df = self.crawler.parse_html_to_dataframe(html_file, 'KOSPI')

        # 검증
        assert not df.is_empty()
        assert len(df) == 2
        assert 'company_name' in df.columns
        assert 'company_code' in df.columns
        assert 'delisting_date' in df.columns
        assert 'delisting_reason' in df.columns
        assert 'market' in df.columns

        # 데이터 내용 검증
        first_row = df.row(0, named=True)
        assert first_row['company_name'] == '테스트회사'
        assert first_row['company_code'] == '123456'
        assert first_row['market'] == 'KOSPI'

    def test_html_parsing_no_table(self):
        """테이블이 없는 HTML 파싱 테스트"""
        html_content = "<html><body><p>No table here</p></body></html>"

        html_file = Path(self.temp_dir) / "no_table.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        df = self.crawler.parse_html_to_dataframe(html_file, 'KOSPI')

        assert df.is_empty()

    def test_column_normalization(self):
        """컬럼명 정규화 테스트"""
        # 테스트 데이터프레임 생성
        df = pl.DataFrame({
            '회사명': ['테스트회사'],
            '종목코드': ['123456'],
            '상장폐지일': ['20231231'],
            '상장폐지사유': ['테스트사유']
        })

        normalized_df = self.crawler._normalize_columns(df, 'KOSPI')

        # 컬럼명 변환 확인
        assert 'company_name' in normalized_df.columns
        assert 'company_code' in normalized_df.columns
        assert 'delisting_date' in normalized_df.columns
        assert 'delisting_reason' in normalized_df.columns
        assert 'market' in normalized_df.columns

        # 시장 정보 추가 확인
        assert normalized_df['market'][0] == 'KOSPI'

    def test_date_column_processing(self):
        """날짜 컬럼 처리 테스트"""
        df = pl.DataFrame({
            'delisting_date': ['2023.12.31', '2023-11-30', '20231025']
        })

        normalized_df = self.crawler._normalize_columns(df, 'KOSPI')

        # 날짜 형식 변환 확인 (날짜 타입으로 변환되어야 함)
        assert normalized_df['delisting_date'].dtype in [pl.Date, pl.Datetime]

    def test_company_code_processing(self):
        """종목코드 처리 테스트"""
        df = pl.DataFrame({
            'company_code': ['A123456', '654321B', '789012', '12345']  # 마지막은 5자리라 필터링됨
        })

        normalized_df = self.crawler._normalize_columns(df, 'KOSPI')

        # 6자리 종목코드만 남아야 함
        assert len(normalized_df) == 3
        codes = normalized_df['company_code'].to_list()
        assert '123456' in codes
        assert '654321' in codes
        assert '789012' in codes

    # 3. 전체 시장 크롤링 통합 테스트

    @patch('src.crawlers.krx_delisted_crawler.KRXDelistedCrawler.crawl_market')
    def test_crawl_all_markets_full_sync(self, mock_crawl_market):
        """전체 시장 크롤링 통합 테스트"""
        # 모의 데이터 설정
        kospi_data = pl.DataFrame({
            'company_name': ['KOSPI회사1', 'KOSPI회사2'],
            'company_code': ['123456', '234567'],
            'delisting_date': [date(2023, 12, 31), date(2023, 11, 30)],
            'delisting_reason': ['사유1', '사유2'],
            'market': ['KOSPI', 'KOSPI']
        })

        kosdaq_data = pl.DataFrame({
            'company_name': ['KOSDAQ회사1'],
            'company_code': ['345678'],
            'delisting_date': [date(2023, 10, 31)],
            'delisting_reason': ['사유3'],
            'market': ['KOSDAQ']
        })

        konex_data = pl.DataFrame()  # 빈 데이터프레임

        def mock_crawl_side_effect(market_name, start_date, end_date):
            if market_name == 'KOSPI':
                return kospi_data
            elif market_name == 'KOSDAQ':
                return kosdaq_data
            elif market_name == 'KONEX':
                return konex_data
            return None

        mock_crawl_market.side_effect = mock_crawl_side_effect

        # 실행
        result_df = self.crawler.crawl_all_markets_full_sync(start_year=1990)

        # 검증
        assert not result_df.is_empty()
        assert len(result_df) == 3  # KOSPI 2개 + KOSDAQ 1개

        # 모든 시장 크롤링 호출 확인
        assert mock_crawl_market.call_count == 3

        # 시장별 데이터 확인
        markets = result_df['market'].unique().to_list()
        assert 'KOSPI' in markets
        assert 'KOSDAQ' in markets

    @patch('src.crawlers.krx_delisted_crawler.KRXDelistedCrawler.crawl_market')
    def test_crawl_all_markets_with_duplicates(self, mock_crawl_market):
        """중복 데이터가 있는 경우 크롤링 테스트"""
        # 중복 데이터 포함 모의 데이터
        duplicate_data = pl.DataFrame({
            'company_name': ['중복회사', '중복회사', '일반회사'],
            'company_code': ['123456', '123456', '234567'],
            'delisting_date': [date(2023, 12, 31), date(2023, 12, 31), date(2023, 11, 30)],
            'delisting_reason': ['사유1', '사유1', '사유2'],
            'market': ['KOSPI', 'KOSPI', 'KOSPI']
        })

        mock_crawl_market.return_value = duplicate_data

        # 실행
        result_df = self.crawler.crawl_all_markets_full_sync()

        # 중복 제거 확인
        assert len(result_df) == 2  # 중복 제거되어 2개만 남아야 함
        unique_codes = result_df['company_code'].unique().to_list()
        assert '123456' in unique_codes
        assert '234567' in unique_codes

    # 4. 데이터 품질 검증 테스트

    def test_data_quality_validation_valid_data(self):
        """유효한 데이터 품질 검증 테스트"""
        valid_df = pl.DataFrame({
            'company_name': ['정상회사1', '정상회사2'],
            'company_code': ['123456', '234567'],
            'delisting_date': [date(2023, 12, 31), date(2023, 11, 30)],
            'delisting_reason': ['정상사유1', '정상사유2'],
            'market': ['KOSPI', 'KOSDAQ']
        })

        # 기본 품질 검증
        assert not valid_df.is_empty()
        assert len(valid_df) == 2

        # 필수 컬럼 존재 확인
        required_columns = ['company_name', 'company_code', 'delisting_date', 'market']
        for col in required_columns:
            assert col in valid_df.columns

        # 종목코드 형식 확인
        codes = valid_df['company_code'].to_list()
        for code in codes:
            assert len(code) == 6
            assert code.isdigit()

    def test_data_quality_validation_invalid_codes(self):
        """잘못된 종목코드 데이터 검증 테스트"""
        invalid_df = pl.DataFrame({
            'company_code': ['12345', 'ABCDEF', '', 'XYZ']  # 모두 잘못된 형식
        })

        # 정규화 과정에서 잘못된 종목코드 필터링
        normalized_df = self.crawler._normalize_columns(invalid_df, 'KOSPI')

        # 유효한 6자리 숫자 종목코드만 남아야 함 (모든 데이터가 잘못되었으므로 0개)
        assert len(normalized_df) == 0  # 모두 필터링되어야 함

    # 5. Parquet 저장 기능 테스트

    def test_save_to_parquet(self):
        """Parquet 파일 저장 테스트"""
        test_df = pl.DataFrame({
            'company_name': ['테스트회사'],
            'company_code': ['123456'],
            'delisting_date': [date(2023, 12, 31)],
            'market': ['KOSPI']
        })

        output_path = self.crawler.save_to_parquet(test_df, 'test_output.parquet')

        # 파일 생성 확인
        assert output_path.exists()
        assert output_path.suffix == '.parquet'

        # 저장된 데이터 검증
        loaded_df = pl.read_parquet(output_path)
        assert len(loaded_df) == 1
        assert loaded_df['company_name'][0] == '테스트회사'

    def test_save_to_parquet_auto_filename(self):
        """자동 파일명 생성 저장 테스트"""
        test_df = pl.DataFrame({
            'company_name': ['테스트회사'],
            'company_code': ['123456']
        })

        output_path = self.crawler.save_to_parquet(test_df)

        # 자동 생성된 파일명 확인
        assert output_path.exists()
        assert 'delisted_stocks_crawled_' in output_path.name
        assert output_path.suffix == '.parquet'

    # 6. ClickHouse 통합 테스트 (모의 테스트)

    @patch('src.clickhouse.client.ClickHouseClient')
    def test_clickhouse_integration_preparation(self, mock_clickhouse_client):
        """ClickHouse 통합을 위한 데이터 준비 테스트"""
        # 모의 ClickHouse 클라이언트 설정
        mock_client = Mock()
        mock_clickhouse_client.return_value = mock_client

        # 테스트 데이터
        delisted_df = pl.DataFrame({
            'company_name': ['상장폐지회사1', '상장폐지회사2'],
            'company_code': ['123456', '234567'],
            'delisting_date': [date(2023, 12, 31), date(2023, 11, 30)],
            'delisting_reason': ['상장폐지사유1', '상장폐지사유2'],
            'market': ['KOSPI', 'KOSDAQ']
        })

        # ClickHouse 클라이언트 생성
        client = mock_clickhouse_client()

        # 데이터 형식이 ClickHouse 업로드에 적합한지 확인
        assert not delisted_df.is_empty()
        assert 'company_code' in delisted_df.columns
        assert 'delisting_date' in delisted_df.columns
        assert delisted_df['delisting_date'].dtype in [pl.Date, pl.Datetime]

        # 모의 업로드 호출
        client.insert_dataframe.return_value = True
        result = client.insert_dataframe('delisted_stocks', delisted_df)

        assert result is True
        client.insert_dataframe.assert_called_once()

    # 7. 에러 처리 및 복구 테스트

    @patch('src.crawlers.krx_delisted_crawler.KRXDelistedCrawler._download_excel_data')
    def test_crawler_error_handling(self, mock_download):
        """크롤러 에러 처리 테스트"""
        # 다운로드 실패 시뮬레이션
        mock_download.return_value = None

        result = self.crawler.crawl_market('KOSPI')

        assert result is None
        mock_download.assert_called_once()

    def test_invalid_market_name(self):
        """잘못된 시장명 처리 테스트"""
        result = self.crawler.crawl_market('INVALID_MARKET')

        assert result is None

    @patch('src.crawlers.krx_delisted_crawler.KRXDelistedCrawler.crawl_market')
    def test_partial_failure_handling(self, mock_crawl_market):
        """일부 시장 크롤링 실패 시 처리 테스트"""
        def side_effect(market_name, start_date, end_date):
            if market_name == 'KOSPI':
                return pl.DataFrame({
                    'company_name': ['성공회사'],
                    'company_code': ['123456'],
                    'market': ['KOSPI']
                })
            elif market_name == 'KOSDAQ':
                raise Exception("KOSDAQ 크롤링 실패")
            else:
                return pl.DataFrame()  # KONEX는 빈 데이터

        mock_crawl_market.side_effect = side_effect

        # 일부 실패가 있어도 성공한 데이터는 반환되어야 함
        result_df = self.crawler.crawl_all_markets_full_sync()

        assert not result_df.is_empty()
        assert len(result_df) == 1
        assert result_df['market'][0] == 'KOSPI'

    # 8. 성능 및 대용량 데이터 처리 테스트

    def test_large_dataset_processing(self):
        """대용량 데이터셋 처리 테스트"""
        # 큰 데이터셋 생성 (1000개 레코드)
        large_df = pl.DataFrame({
            'company_name': [f'회사{i}' for i in range(1000)],
            'company_code': [f'{i:06d}' for i in range(100000, 101000)],
            'delisting_date': [date(2023, 12, 31)] * 1000,
            'delisting_reason': ['대량테스트'] * 1000,
            'market': ['KOSPI'] * 500 + ['KOSDAQ'] * 500
        })

        # 정규화 처리
        normalized_df = self.crawler._normalize_columns(large_df, 'KOSPI')

        # 처리 결과 확인
        assert len(normalized_df) == 1000
        assert 'market' in normalized_df.columns

    # 9. 통합 시나리오 테스트

    @patch('src.crawlers.krx_delisted_crawler.KRXDelistedCrawler._download_excel_data')
    def test_end_to_end_scenario(self, mock_download):
        """전체 프로세스 통합 시나리오 테스트"""
        # 모의 HTML 파일 생성
        html_content = """
        <html><body>
        <table>
            <tr><th>회사명</th><th>종목코드</th><th>상장폐지일</th><th>상장폐지사유</th></tr>
            <tr><td>통합테스트회사</td><td>999999</td><td>2023.12.31</td><td>통합테스트</td></tr>
        </table>
        </body></html>
        """

        html_file = Path(self.temp_dir) / "integration_test.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        mock_download.return_value = html_file

        # 전체 프로세스 실행
        result_df = self.crawler.crawl_market('KOSPI')

        # 결과 검증
        assert not result_df.is_empty()
        assert len(result_df) == 1
        assert result_df['company_name'][0] == '통합테스트회사'
        assert result_df['company_code'][0] == '999999'
        assert result_df['market'][0] == 'KOSPI'

        # Parquet 저장 테스트
        output_path = self.crawler.save_to_parquet(result_df, 'integration_test.parquet')
        assert output_path.exists()

        # 저장된 파일 검증
        loaded_df = pl.read_parquet(output_path)
        assert len(loaded_df) == 1
        assert loaded_df['company_name'][0] == '통합테스트회사'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])