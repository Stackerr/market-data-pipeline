#!/usr/bin/env python3
"""
KRX 신규상장 크롤러 테스트
"""

import pytest
import polars as pl
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from datetime import datetime, timedelta

from src.crawlers.krx_new_listing_crawler import KRXNewListingCrawler


class TestKRXNewListingCrawler:
    """KRX 신규상장 크롤러 테스트"""

    @pytest.fixture
    def crawler(self, tmp_path):
        """크롤러 인스턴스 생성"""
        return KRXNewListingCrawler(data_dir=str(tmp_path))

    @pytest.fixture
    def sample_html(self):
        """샘플 HTML 데이터"""
        return """
        <table>
            <tr>
                <th>회사명</th>
                <th>종목코드</th>
                <th>상장일</th>
                <th>시장구분</th>
                <th>상장유형</th>
                <th>업종</th>
            </tr>
            <tr>
                <td>테스트회사A</td>
                <td>123456</td>
                <td>2024.01.15</td>
                <td>KOSPI</td>
                <td>신규상장</td>
                <td>제조업</td>
            </tr>
            <tr>
                <td>테스트회사B</td>
                <td>789012</td>
                <td>2024.01.20</td>
                <td>KOSDAQ</td>
                <td>신규상장</td>
                <td>IT</td>
            </tr>
        </table>
        """

    def test_crawler_initialization(self, tmp_path):
        """크롤러 초기화 테스트"""
        crawler = KRXNewListingCrawler(data_dir=str(tmp_path))

        assert crawler.base_url == "https://kind.krx.co.kr/listinvstg/listingcompany.do"
        assert crawler.data_dir == tmp_path
        assert 'KOSPI' in crawler.market_codes
        assert 'KOSDAQ' in crawler.market_codes
        assert 'KONEX' in crawler.market_codes

    def test_get_search_form_data_default(self, crawler):
        """기본 검색 폼 데이터 생성 테스트"""
        form_data = crawler._get_search_form_data('stockMkt', 'NEW')

        assert form_data['method'] == 'searchListingTypeList'
        assert form_data['stockMkt'] == 'on'
        assert form_data['listingType1'] == 'on'  # 신규상장
        assert 'listingType2' not in form_data
        assert form_data['currentPageSize'] == '5000'

    def test_get_search_form_data_custom_dates(self, crawler):
        """커스텀 날짜 검색 폼 데이터 테스트"""
        start_date = "20240101"
        end_date = "20240131"

        form_data = crawler._get_search_form_data('kosdaqMkt', 'TRANSFER', start_date, end_date)

        assert form_data['fromDate'] == start_date
        assert form_data['toDate'] == end_date
        assert form_data['kosdaqMkt'] == 'on'
        assert form_data['listingType2'] == 'on'  # 이전상장

    def test_get_search_form_data_all_types(self, crawler):
        """모든 상장 유형 선택 테스트"""
        form_data = crawler._get_search_form_data('konexMkt', 'ALL')

        assert form_data['konexMkt'] == 'on'
        assert form_data['listingType1'] == 'on'  # 신규상장
        assert form_data['listingType2'] == 'on'  # 이전상장
        assert form_data['listingType3'] == 'on'  # 재상장

    def test_parse_html_to_dataframe(self, crawler, sample_html, tmp_path):
        """HTML 파싱 테스트"""
        # 임시 HTML 파일 생성
        html_file = tmp_path / "test_new_listing.html"
        html_file.write_text(sample_html, encoding='utf-8')

        df = crawler.parse_html_to_dataframe(html_file, "KOSPI")

        assert not df.is_empty()
        assert len(df) == 2
        assert 'company_name' in df.columns
        assert 'company_code' in df.columns
        assert 'listing_date' in df.columns
        assert 'market' in df.columns

        # 첫 번째 행 데이터 확인
        first_row = df.row(0, named=True)
        assert first_row['company_name'] == '테스트회사A'
        assert first_row['company_code'] == '123456'
        assert first_row['market'] == 'KOSPI'

    def test_normalize_columns(self, crawler):
        """컬럼 정규화 테스트"""
        # 테스트 데이터 생성
        test_data = {
            '회사명': ['테스트회사'],
            '종목코드': ['123456'],
            '상장일': ['20240115'],
            '액면가': ['500원'],
            '공모가격': ['10,000원']
        }
        df = pl.DataFrame(test_data)

        normalized_df = crawler._normalize_columns(df, "KOSPI")

        # 컬럼명 변경 확인
        assert 'company_name' in normalized_df.columns
        assert 'company_code' in normalized_df.columns
        assert 'listing_date' in normalized_df.columns
        assert 'market' in normalized_df.columns

        # 데이터 타입 확인
        assert normalized_df['company_code'].dtype == pl.String
        assert normalized_df['listing_date'].dtype == pl.Date

        # 시장 정보 추가 확인
        assert normalized_df['market'][0] == 'KOSPI'

    def test_normalize_columns_invalid_stock_code(self, crawler):
        """잘못된 종목코드 필터링 테스트"""
        test_data = {
            '회사명': ['회사A', '회사B', '회사C'],
            '종목코드': ['123456', '12345', 'ABC123']  # 6자리, 5자리, 문자포함
        }
        df = pl.DataFrame(test_data)

        normalized_df = crawler._normalize_columns(df, "KOSPI")

        # 6자리 종목코드만 남아있어야 함
        assert len(normalized_df) == 1
        assert normalized_df['company_code'][0] == '123456'

    def test_normalize_columns_numeric_fields(self, crawler):
        """숫자 필드 변환 테스트"""
        test_data = {
            '회사명': ['테스트회사'],
            '종목코드': ['123456'],
            '액면가': ['500원'],
            '공모가격': ['10,000원'],
            '공모금액': ['1,000억원'],
            '상장주식수': ['1,000,000주']
        }
        df = pl.DataFrame(test_data)

        normalized_df = crawler._normalize_columns(df, "KOSPI")

        # 숫자 변환 확인
        assert normalized_df['par_value'][0] == 500.0
        assert normalized_df['ipo_price'][0] == 10000.0

    @patch('requests.Session.get')
    @patch('requests.Session.post')
    def test_download_excel_data_success(self, mock_post, mock_get, crawler):
        """Excel 데이터 다운로드 성공 테스트"""
        # Mock 응답 설정
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status.return_value = None

        mock_post.return_value.status_code = 200
        mock_post.return_value.raise_for_status.return_value = None
        mock_post.return_value.content = "테스트 HTML 내용".encode('utf-8')

        # 다운로드 실행
        result = crawler._download_excel_data('stockMkt', 'KOSPI')

        assert result is not None
        assert result.exists()
        assert 'kospi_new_listing' in result.name

    @patch('requests.Session.get')
    def test_download_excel_data_failure(self, mock_get, crawler):
        """Excel 데이터 다운로드 실패 테스트"""
        # HTTP 에러 발생 시뮬레이션
        mock_get.side_effect = Exception("Network error")

        result = crawler._download_excel_data('stockMkt', 'KOSPI')

        assert result is None

    def test_crawl_market_invalid_market(self, crawler):
        """잘못된 시장명으로 크롤링 테스트"""
        result = crawler.crawl_market('INVALID_MARKET')

        assert result is None

    @patch.object(KRXNewListingCrawler, '_download_excel_data')
    @patch.object(KRXNewListingCrawler, 'parse_html_to_dataframe')
    def test_crawl_market_success(self, mock_parse, mock_download, crawler, tmp_path):
        """시장별 크롤링 성공 테스트"""
        # Mock 설정
        html_file = tmp_path / "test.html"
        html_file.touch()
        mock_download.return_value = html_file

        test_df = pl.DataFrame({
            'company_name': ['테스트회사'],
            'company_code': ['123456'],
            'market': ['KOSPI']
        })
        mock_parse.return_value = test_df

        result = crawler.crawl_market('KOSPI')

        assert result is not None
        assert not result.is_empty()
        assert len(result) == 1

    @patch.object(KRXNewListingCrawler, 'crawl_market')
    def test_crawl_recent_listings(self, mock_crawl_market, crawler):
        """최근 상장 종목 크롤링 테스트"""
        # Mock 데이터 설정
        kospi_df = pl.DataFrame({
            'company_name': ['KOSPI회사'],
            'company_code': ['123456'],
            'market': ['KOSPI']
        })
        kosdaq_df = pl.DataFrame({
            'company_name': ['KOSDAQ회사'],
            'company_code': ['789012'],
            'market': ['KOSDAQ']
        })

        # 시장별로 다른 결과 반환
        mock_crawl_market.side_effect = [kospi_df, kosdaq_df, pl.DataFrame()]

        result = crawler.crawl_recent_listings(days=7)

        assert not result.is_empty()
        assert len(result) == 2
        assert mock_crawl_market.call_count == 3  # KOSPI, KOSDAQ, KONEX

    @patch.object(KRXNewListingCrawler, 'crawl_market')
    def test_crawl_recent_listings_no_data(self, mock_crawl_market, crawler):
        """최근 상장 종목 없음 테스트"""
        # 모든 시장에서 빈 DataFrame 반환
        mock_crawl_market.return_value = pl.DataFrame()

        result = crawler.crawl_recent_listings(days=7)

        assert result.is_empty()

    def test_save_to_parquet(self, crawler):
        """Parquet 저장 테스트"""
        test_df = pl.DataFrame({
            'company_name': ['테스트회사'],
            'company_code': ['123456'],
            'market': ['KOSPI']
        })

        output_path = crawler.save_to_parquet(test_df, "test_new_listings.parquet")

        assert output_path.exists()
        assert output_path.suffix == '.parquet'

        # 저장된 파일 읽어서 확인
        loaded_df = pl.read_parquet(output_path)
        assert len(loaded_df) == 1
        assert loaded_df['company_name'][0] == '테스트회사'

    def test_save_to_parquet_auto_filename(self, crawler):
        """자동 파일명 생성 테스트"""
        test_df = pl.DataFrame({
            'company_name': ['테스트회사'],
            'company_code': ['123456']
        })

        output_path = crawler.save_to_parquet(test_df)

        assert output_path.exists()
        assert 'new_listings_crawled_' in output_path.name
        assert output_path.suffix == '.parquet'