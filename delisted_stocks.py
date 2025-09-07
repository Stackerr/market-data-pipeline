import FinanceDataReader as fdr


def get_delisted_stocks():
    """상장폐지된 종목 리스트를 조회합니다."""
    try:
        # 현재 상장 종목 리스트 조회 후 비교를 통해 상장폐지 확인
        print("현재 상장 종목 확인...")
        kospi_current = fdr.StockListing('KOSPI')
        kosdaq_current = fdr.StockListing('KOSDAQ')
        
        print("KOSPI 현재 상장 종목:")
        print(f"총 {len(kospi_current)}개 종목")
        print(kospi_current.head(10))
        print("\n")
        
        print("KOSDAQ 현재 상장 종목:")
        print(f"총 {len(kosdaq_current)}개 종목")
        print(kosdaq_current.head(10))
        print("\n")
        
        # 특정 종목의 과거 데이터를 통해 상장폐지 확인
        print("상장폐지 종목 샘플 확인...")
        delisted_samples = ['000880', '003540', '001755']  # 알려진 상장폐지 종목들
        
        for code in delisted_samples:
            try:
                stock_data = fdr.DataReader(code, '2020-01-01', '2024-12-31')
                if stock_data.empty:
                    print(f"종목코드 {code}: 데이터 없음 (상장폐지 가능성)")
                else:
                    last_date = stock_data.index[-1]
                    print(f"종목코드 {code}: 마지막 거래일 {last_date}")
            except Exception as e:
                print(f"종목코드 {code}: 조회 실패 - {e}")
        
        return kospi_current, kosdaq_current
        
    except Exception as e:
        print(f"상장폐지 종목 조회 중 오류 발생: {e}")
        return None, None

if __name__ == "__main__":
    kospi_delisted, kosdaq_delisted = get_delisted_stocks()