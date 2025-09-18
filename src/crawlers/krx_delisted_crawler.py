#!/usr/bin/env python3
"""
KRX 상장폐지 종목 크롤러
https://kind.krx.co.kr/investwarn/delcompany.do?method=searchDelCompanyMain
"""

import requests
import polars as pl
from bs4 import BeautifulSoup
import logging
import time
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KRXDelistedCrawler:
    """KRX 상장폐지 종목 크롤러"""

    def __init__(self, data_dir: str = "data/raw"):
        self.base_url = "https://kind.krx.co.kr/investwarn/delcompany.do"
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 세션 생성
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        # 시장 구분 코드 매핑
        self.market_codes = {
            'KOSPI': 'Y',      # 유가증권시장
            'KOSDAQ': 'K',     # 코스닥시장
            'KONEX': 'N'       # 코넥스시장
        }

    def _get_search_form_data(self, market_code: str, start_date: str = "19900101", end_date: str = None) -> Dict[str, str]:
        """검색 폼 데이터 생성"""
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        return {
            'method': 'searchDelCompanyList',
            'currentPageSize': '5000',  # 큰 페이지 사이즈로 모든 데이터 가져오기
            'pageIndex': '1',
            'orderMode': '0',
            'orderStat': 'D',
            'market': market_code,
            'fromDate': start_date,
            'toDate': end_date,
            'delGubun': '',  # 전체
            'companyNm': '',  # 회사명 미지정
        }

    def _download_excel_data(self, market_code: str, market_name: str, start_date: str = "19900101", end_date: str = None) -> Optional[Path]:
        """Excel 형식으로 데이터 다운로드 (실제로는 HTML 파일)"""
        try:
            # 1. 먼저 메인 페이지 접속으로 세션 설정
            logger.info(f"Accessing main page for {market_name}...")
            main_response = self.session.get(f"{self.base_url}?method=searchDelCompanyMain")
            main_response.raise_for_status()

            # 잠시 대기
            time.sleep(1)

            # 2. 검색 폼 데이터 준비
            form_data = self._get_search_form_data(market_code, start_date, end_date)

            # 3. 검색 실행
            logger.info(f"Executing search for {market_name} market...")
            search_response = self.session.post(self.base_url, data=form_data)
            search_response.raise_for_status()

            # 검색 결과 확인
            search_content = search_response.text
            if "오류" in search_content or "error" in search_content.lower():
                logger.warning(f"Search returned error page for {market_name}")
                # 에러가 있어도 계속 시도

            time.sleep(2)

            # 4. Excel 다운로드 (POST 방식으로 시도)
            excel_data = form_data.copy()
            excel_data['method'] = 'searchDelCompanyExcel'

            logger.info(f"Downloading Excel data for {market_name}...")
            excel_response = self.session.post(self.base_url, data=excel_data)
            excel_response.raise_for_status()

            # 5. 파일 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{market_name.lower()}_delisted_{timestamp}.html"
            file_path = self.data_dir / filename

            # 인코딩 처리 (KRX는 보통 EUC-KR 사용)
            content = ""
            try:
                content = excel_response.content.decode('euc-kr')
            except UnicodeDecodeError:
                try:
                    content = excel_response.content.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        content = excel_response.content.decode('cp949')
                    except UnicodeDecodeError:
                        content = excel_response.content.decode('latin1')

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

            logger.info(f"✅ {market_name} data downloaded: {file_path} ({len(content):,} characters)")

            # 내용 확인
            if "오류" in content or "error" in content.lower() or len(content) < 1000:
                logger.warning(f"Downloaded content might be an error page for {market_name}")

            return file_path

        except Exception as e:
            logger.error(f"❌ Failed to download {market_name} data: {e}")
            return None

    def parse_html_to_dataframe(self, html_file: Path, market_name: str) -> pl.DataFrame:
        """HTML 파일을 파싱하여 DataFrame으로 변환"""
        try:
            logger.info(f"Parsing HTML file: {html_file}")

            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, 'html.parser')

            # 테이블 찾기
            tables = soup.find_all('table')
            if not tables:
                logger.warning(f"No tables found in {html_file}")
                return pl.DataFrame()

            # 가장 큰 테이블을 데이터 테이블로 가정
            main_table = max(tables, key=lambda t: len(t.find_all('tr')))
            rows = main_table.find_all('tr')

            if len(rows) < 2:
                logger.warning(f"Not enough rows in table: {len(rows)}")
                return pl.DataFrame()

            # 헤더 추출
            header_row = rows[0]
            headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
            logger.info(f"Found headers: {headers}")

            # 데이터 추출
            data_rows = []
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= len(headers):
                    row_data = [cell.get_text().strip() for cell in cells[:len(headers)]]
                    data_rows.append(row_data)

            if not data_rows:
                logger.warning(f"No data rows found in {html_file}")
                return pl.DataFrame()

            # DataFrame 생성
            df_data = {header: [] for header in headers}
            for row in data_rows:
                for i, header in enumerate(headers):
                    df_data[header].append(row[i] if i < len(row) else '')

            df = pl.DataFrame(df_data)

            # 컬럼명 정규화
            df = self._normalize_columns(df, market_name)

            logger.info(f"✅ Parsed {len(df)} records from {html_file}")
            return df

        except Exception as e:
            logger.error(f"❌ Failed to parse {html_file}: {e}")
            return pl.DataFrame()

    def _normalize_columns(self, df: pl.DataFrame, market_name: str) -> pl.DataFrame:
        """컬럼명 정규화 및 데이터 타입 변환"""
        try:
            # 일반적인 컬럼명 매핑
            column_mapping = {
                '회사명': 'company_name',
                '종목코드': 'company_code',
                '상장폐지일': 'delisting_date',
                '상장폐지사유': 'delisting_reason',
                '비고': 'remarks',
                '순번': 'sequence'
            }

            # 컬럼명 변경
            current_columns = df.columns
            rename_dict = {}
            for old_col in current_columns:
                for kr_name, en_name in column_mapping.items():
                    if kr_name in old_col:
                        rename_dict[old_col] = en_name
                        break

            if rename_dict:
                df = df.rename(rename_dict)
                logger.info(f"Renamed columns: {rename_dict}")

            # 시장 정보 추가
            df = df.with_columns(pl.lit(market_name).alias('market'))

            # 날짜 컬럼 처리
            if 'delisting_date' in df.columns:
                df = df.with_columns(
                    pl.col('delisting_date')
                    .str.replace_all(r'[^\d]', '')  # 숫자가 아닌 문자 제거
                    .str.strptime(pl.Date, format='%Y%m%d', strict=False)
                    .alias('delisting_date')
                )

            # 종목코드 정리 (6자리 숫자만)
            if 'company_code' in df.columns:
                df = df.with_columns(
                    pl.col('company_code')
                    .str.replace_all(r'[^\d]', '')  # 숫자가 아닌 문자 제거
                    .str.slice(0, 6)  # 처음 6자리만
                    .alias('company_code')
                )

                # 6자리가 아닌 종목코드 필터링
                df = df.filter(pl.col('company_code').str.len_chars() == 6)

            # 빈 문자열을 null로 변환
            for col in df.columns:
                if df[col].dtype == pl.String:
                    df = df.with_columns(
                        pl.col(col).replace('', None)
                    )

            return df

        except Exception as e:
            logger.error(f"Failed to normalize columns: {e}")
            return df

    def crawl_market(self, market_name: str, start_date: str = "19900101", end_date: str = None) -> Optional[pl.DataFrame]:
        """특정 시장의 상장폐지 종목 크롤링"""
        if market_name not in self.market_codes:
            logger.error(f"Invalid market name: {market_name}. Valid options: {list(self.market_codes.keys())}")
            return None

        market_code = self.market_codes[market_name]

        logger.info(f"🚀 Starting crawl for {market_name} market (code: {market_code})")

        # 1. 데이터 다운로드
        html_file = self._download_excel_data(market_code, market_name, start_date, end_date)
        if not html_file:
            return None

        # 잠시 대기 (서버 부하 방지)
        time.sleep(2)

        # 2. HTML 파싱
        df = self.parse_html_to_dataframe(html_file, market_name)

        return df

    def crawl_all_markets(self, start_date: str = "19900101", end_date: str = None) -> pl.DataFrame:
        """모든 시장의 상장폐지 종목 크롤링"""
        logger.info("🚀 Starting crawl for all markets...")

        all_dfs = []

        for market_name in self.market_codes.keys():
            try:
                df = self.crawl_market(market_name, start_date, end_date)
                if df is not None and not df.is_empty():
                    all_dfs.append(df)
                    logger.info(f"✅ {market_name}: {len(df)} records")
                else:
                    logger.warning(f"⚠️ {market_name}: No data")

                # 시장 간 요청 간격
                time.sleep(3)

            except Exception as e:
                logger.error(f"❌ Failed to crawl {market_name}: {e}")

        if not all_dfs:
            logger.warning("No data crawled from any market")
            return pl.DataFrame()

        # 모든 데이터 결합
        combined_df = pl.concat(all_dfs, how="vertical_relaxed")

        # 중복 제거 (종목코드 + 상장폐지일 기준)
        if 'company_code' in combined_df.columns and 'delisting_date' in combined_df.columns:
            combined_df = combined_df.unique(subset=['company_code', 'delisting_date'])

        logger.info(f"🎯 Total crawled records: {len(combined_df)}")
        return combined_df

    def save_to_parquet(self, df: pl.DataFrame, filename: str = None) -> Path:
        """DataFrame을 Parquet 파일로 저장"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"delisted_stocks_crawled_{timestamp}.parquet"

        output_path = self.data_dir / filename
        df.write_parquet(output_path)

        logger.info(f"💾 Data saved to: {output_path}")
        return output_path


def main():
    """메인 실행 함수"""
    crawler = KRXDelistedCrawler()

    try:
        # 모든 시장 크롤링
        df = crawler.crawl_all_markets()

        if not df.is_empty():
            # Parquet으로 저장
            output_path = crawler.save_to_parquet(df)

            # 결과 요약
            logger.info("📊 Crawling Summary:")
            logger.info(f"  Total records: {len(df)}")

            if 'market' in df.columns:
                market_counts = df.group_by('market').agg(pl.count().alias('count'))
                for row in market_counts.iter_rows(named=True):
                    logger.info(f"  {row['market']}: {row['count']} records")

            return True
        else:
            logger.warning("No data was crawled")
            return False

    except Exception as e:
        logger.error(f"❌ Crawling failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)