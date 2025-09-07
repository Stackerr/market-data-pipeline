import FinanceDataReader as fdr

import polars as pl

def explore_fdr_api():
    """FinanceDataReader API 탐색"""
    
    print("=== FinanceDataReader API 분석 ===\n")
    
    # 1. 현재 상장 종목 리스트
    print("1. 현재 상장 종목 리스트:")
    try:
        # KOSPI 상장 종목
        kospi_stocks = pl.DataFrame(fdr.StockListing('KOSPI'))

        print(f"KOSPI 종목 수: {len(kospi_stocks)}")
        print("KOSPI 컬럼:", kospi_stocks.columns.tolist())
        print("KOSPI 샘플:")
        print(kospi_stocks.head(3))
        print()
        
        # KOSDAQ 상장 종목  
        kosdaq_stocks = fdr.StockListing('KOSDAQ')
        print(f"KOSDAQ 종목 수: {len(kosdaq_stocks)}")
        print("KOSDAQ 컬럼:", kosdaq_stocks.columns.tolist())
        print("KOSDAQ 샘플:")
        print(kosdaq_stocks.head(3))
        print()
        
    except Exception as e:
        print(f"상장 종목 조회 오류: {e}")
    
    # 2. 개별 종목 데이터 확인
    print("2. 개별 종목 데이터:")
    try:
        # 삼성전자 데이터 (최근 10일)
        samsung = fdr.DataReader('005930', '2024-01-01', '2024-01-10')
        print("삼성전자 데이터 컬럼:", samsung.columns.tolist())
        print("삼성전자 샘플:")
        print(samsung.head(3))
        print()
    except Exception as e:
        print(f"개별 종목 데이터 오류: {e}")
    
    # 3. 지수 데이터
    print("3. 지수 데이터:")
    try:
        kospi_idx = fdr.DataReader('KS11', '2024-01-01', '2024-01-10')  # KOSPI 지수
        print("KOSPI 지수 컬럼:", kospi_idx.columns.tolist())
        print()
    except Exception as e:
        print(f"지수 데이터 오류: {e}")
    
    # 4. 기타 데이터 소스들
    print("4. 기타 데이터 소스 확인:")
    try:
        # 한국 상장 기업 전체 (KRX)
        all_stocks = fdr.StockListing('KRX')
        print(f"KRX 전체 종목 수: {len(all_stocks)}")
        print("KRX 컬럼:", all_stocks.columns.tolist())
        print("KRX 샘플:")
        print(all_stocks.head(3))
        print()
        
        # 시장 구분 확인
        print("시장 구분별 종목 수:")
        if 'Market' in all_stocks.columns:
            print(all_stocks['Market'].value_counts())
        elif 'market' in all_stocks.columns:
            print(all_stocks['market'].value_counts())
        print()
        
    except Exception as e:
        print(f"KRX 데이터 오류: {e}")

if __name__ == "__main__":
    explore_fdr_api()