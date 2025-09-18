#!/usr/bin/env python3
"""
초기 설정 스크립트 테스트 (TDD)
Phase 1.1: 상장일 정보 수집 시스템
"""

import pytest
import polars as pl
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, date
import tempfile

from scripts.initial_setup import InitialDataSetup


class TestPhase11ListingDateCollection:
    """Phase 1.1: 상장일 정보 수집 테스트"""

    @pytest.fixture
    def setup_instance(self):
        """테스트용 InitialDataSetup 인스턴스"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('scripts.initial_setup.project_root', Path(temp_dir)):
                setup = InitialDataSetup()
                yield setup

    @pytest.fixture
    def mock_listing_data(self):
        """모의 상장 데이터"""
        return pl.DataFrame({
            'company_code': ['005930', '000660', '035720'],
            'company_name': ['삼성전자', 'SK하이닉스', '카카오'],
            'listing_date': [date(2020, 1, 15), date(2021, 3, 10), date(2022, 5, 20)],
            'market': ['KOSPI', 'KOSPI', 'KOSDAQ'],
            'listing_type': ['신규상장', '신규상장', '신규상장']
        })

    def test_step_1_1_should_fail_when_no_crawler_data(self, setup_instance):
        """1.1 테스트: 크롤러 데이터가 없을 때 실패해야 함"""
        # Given: 크롤러가 빈 데이터 반환
        with patch.object(setup_instance.new_listing_crawler, 'crawl_all_listings_full_sync') as mock_crawler:
            mock_crawler.return_value = pl.DataFrame()

            # When: step_1_1 실행
            result = setup_instance.step_1_1_populate_listing_dates(2000)

            # Then: 실패 반환
            assert result is False
            mock_crawler.assert_called_once_with(start_year=2000)

    def test_step_1_1_should_save_backup_file(self, setup_instance, mock_listing_data):
        """1.1 테스트: 백업 파일이 저장되어야 함"""
        # Given: 크롤러가 정상 데이터 반환
        with patch.object(setup_instance.new_listing_crawler, 'crawl_all_listings_full_sync') as mock_crawler, \
             patch.object(setup_instance.stock_master, 'process_new_listings') as mock_process:

            mock_crawler.return_value = mock_listing_data
            mock_process.return_value = {'added': 3, 'skipped': 0, 'errors': 0}

            # When: step_1_1 실행
            result = setup_instance.step_1_1_populate_listing_dates(2000)

            # Then: 성공하고 백업 파일 존재
            assert result is True
            backup_files = list(setup_instance.data_dir.glob("initial_listings_2000_*.parquet"))
            assert len(backup_files) == 1

    def test_step_1_1_should_process_listings_correctly(self, setup_instance, mock_listing_data):
        """1.1 테스트: 상장 데이터가 올바르게 처리되어야 함"""
        # Given: 크롤러가 정상 데이터 반환
        with patch.object(setup_instance.new_listing_crawler, 'crawl_all_listings_full_sync') as mock_crawler, \
             patch.object(setup_instance.stock_master, 'process_new_listings') as mock_process:

            mock_crawler.return_value = mock_listing_data
            mock_process.return_value = {'added': 3, 'skipped': 0, 'errors': 0}

            # When: step_1_1 실행
            result = setup_instance.step_1_1_populate_listing_dates(2000)

            # Then: stock_master.process_new_listings 호출됨
            assert result is True
            mock_process.assert_called_once()

            # 전달된 데이터 검증
            call_args = mock_process.call_args[0][0]
            assert len(call_args) == 3
            assert '005930' in call_args['company_code'].to_list()

    def test_step_1_1_should_fail_when_errors_occur(self, setup_instance, mock_listing_data):
        """1.1 테스트: 처리 중 에러 발생 시 실패해야 함"""
        # Given: 처리 중 에러 발생
        with patch.object(setup_instance.new_listing_crawler, 'crawl_all_listings_full_sync') as mock_crawler, \
             patch.object(setup_instance.stock_master, 'process_new_listings') as mock_process:

            mock_crawler.return_value = mock_listing_data
            mock_process.return_value = {'added': 1, 'skipped': 0, 'errors': 2}

            # When: step_1_1 실행
            result = setup_instance.step_1_1_populate_listing_dates(2000)

            # Then: 실패 반환
            assert result is False

    def test_step_1_1_should_handle_crawler_exception(self, setup_instance):
        """1.1 테스트: 크롤러 예외 처리"""
        # Given: 크롤러에서 예외 발생
        with patch.object(setup_instance.new_listing_crawler, 'crawl_all_listings_full_sync') as mock_crawler:
            mock_crawler.side_effect = Exception("Network error")

            # When: step_1_1 실행
            result = setup_instance.step_1_1_populate_listing_dates(2000)

            # Then: 실패 반환
            assert result is False

    def test_step_1_1_should_use_correct_start_year(self, setup_instance, mock_listing_data):
        """1.1 테스트: 시작 연도가 올바르게 전달되어야 함"""
        # Given: 특정 시작 연도
        start_year = 2010
        with patch.object(setup_instance.new_listing_crawler, 'crawl_all_listings_full_sync') as mock_crawler, \
             patch.object(setup_instance.stock_master, 'process_new_listings') as mock_process:

            mock_crawler.return_value = mock_listing_data
            mock_process.return_value = {'added': 3, 'skipped': 0, 'errors': 0}

            # When: step_1_1 실행
            setup_instance.step_1_1_populate_listing_dates(start_year)

            # Then: 크롤러에 올바른 연도 전달
            mock_crawler.assert_called_once_with(start_year=start_year)

    def test_step_1_1_should_log_results_correctly(self, setup_instance, mock_listing_data, caplog):
        """1.1 테스트: 결과가 올바르게 로깅되어야 함"""
        # Given: 정상 처리 결과
        with patch.object(setup_instance.new_listing_crawler, 'crawl_all_listings_full_sync') as mock_crawler, \
             patch.object(setup_instance.stock_master, 'process_new_listings') as mock_process:

            mock_crawler.return_value = mock_listing_data
            expected_stats = {'added': 2, 'skipped': 1, 'errors': 0}
            mock_process.return_value = expected_stats

            # When: step_1_1 실행 (로거 레벨 설정)
            import logging
            caplog.set_level(logging.INFO)
            setup_instance.step_1_1_populate_listing_dates(2000)

            # Then: 통계가 로그에 기록됨
            assert "Added: 2" in caplog.text
            assert "Skipped (existing): 1" in caplog.text
            assert "Errors: 0" in caplog.text


class TestInitialDataSetupIntegration:
    """InitialDataSetup 통합 테스트"""

    @pytest.fixture
    def setup_instance(self):
        """테스트용 InitialDataSetup 인스턴스"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('scripts.initial_setup.project_root', Path(temp_dir)):
                setup = InitialDataSetup()
                yield setup

    def test_initialization_should_create_data_directory(self, setup_instance):
        """초기화 시 데이터 디렉토리가 생성되어야 함"""
        assert setup_instance.data_dir.exists()
        assert setup_instance.data_dir.is_dir()

    def test_initialization_should_setup_components(self, setup_instance):
        """초기화 시 필요한 컴포넌트가 설정되어야 함"""
        assert hasattr(setup_instance, 'stock_master')
        assert hasattr(setup_instance, 'new_listing_crawler')
        assert hasattr(setup_instance, 'delisted_crawler')

    def test_run_full_initial_setup_should_execute_all_steps(self, setup_instance):
        """전체 초기 설정 실행 시 모든 단계가 호출되어야 함"""
        # Given: 모든 단계 메서드 모킹
        with patch.object(setup_instance, 'step_1_1_populate_listing_dates') as mock_1_1, \
             patch.object(setup_instance, 'step_1_2_populate_delisted_stocks') as mock_1_2, \
             patch.object(setup_instance, 'step_1_3_populate_price_data') as mock_1_3, \
             patch.object(setup_instance, 'step_1_4_infer_listing_dates_pre_2000') as mock_1_4, \
             patch.object(setup_instance, 'step_1_5_infer_delisted_listing_dates') as mock_1_5, \
             patch.object(setup_instance.stock_master, 'get_stock_count') as mock_stats:

            # 모든 단계 성공으로 설정
            mock_1_1.return_value = True
            mock_1_2.return_value = True
            mock_1_3.return_value = True
            mock_1_4.return_value = True
            mock_1_5.return_value = True
            mock_stats.return_value = {'KOSPI': {'active': 100, 'delisted': 50, 'total': 150}}

            # When: 전체 설정 실행
            result = setup_instance.run_full_initial_setup(2000, 1990)

            # Then: 모든 단계 호출되고 성공
            assert result is True
            mock_1_1.assert_called_once_with(2000)
            mock_1_2.assert_called_once_with(1990)
            mock_1_3.assert_called_once()
            mock_1_4.assert_called_once()
            mock_1_5.assert_called_once()

    def test_run_full_initial_setup_should_fail_if_any_step_fails(self, setup_instance):
        """전체 초기 설정에서 한 단계라도 실패하면 전체 실패"""
        # Given: 한 단계 실패
        with patch.object(setup_instance, 'step_1_1_populate_listing_dates') as mock_1_1, \
             patch.object(setup_instance, 'step_1_2_populate_delisted_stocks') as mock_1_2, \
             patch.object(setup_instance, 'step_1_3_populate_price_data') as mock_1_3, \
             patch.object(setup_instance, 'step_1_4_infer_listing_dates_pre_2000') as mock_1_4, \
             patch.object(setup_instance, 'step_1_5_infer_delisted_listing_dates') as mock_1_5, \
             patch.object(setup_instance.stock_master, 'get_stock_count') as mock_stats:

            mock_1_1.return_value = True
            mock_1_2.return_value = False  # 실패
            mock_1_3.return_value = True
            mock_1_4.return_value = True
            mock_1_5.return_value = True
            mock_stats.return_value = {}

            # When: 전체 설정 실행
            result = setup_instance.run_full_initial_setup()

            # Then: 전체 실패
            assert result is False