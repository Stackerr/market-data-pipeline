import FinanceDataReader as fdr
import pandas as pd

# KRX 상장폐지 종목 전체 리스트 가져오기
print("KRX 상장폐지 종목 데이터를 가져오는 중...")
delisted_stocks = fdr.StockListing('KRX-DELISTING')

print(f"총 상장폐지 종목 수: {len(delisted_stocks)}")
print("\n데이터 구조:")
print(delisted_stocks.info())

print("\n첫 10개 종목:")
print(delisted_stocks.head(10))

print("\n컬럼 정보:")
print(delisted_stocks.columns.tolist())

print("\n마지막 10개 종목:")
print(delisted_stocks.tail(10))

# 상장폐지 연도별 통계
if 'ListingDate' in delisted_stocks.columns:
    delisted_stocks['ListingDate'] = pd.to_datetime(delisted_stocks['ListingDate'])
    yearly_stats = delisted_stocks.groupby(delisted_stocks['ListingDate'].dt.year).size()
    print("\n연도별 상장폐지 종목 수:")
    print(yearly_stats.tail(10))

# 데이터를 CSV로 저장
delisted_stocks.to_csv('delisted_stocks_data.csv', index=False, encoding='utf-8-sig')
print("\n데이터가 'delisted_stocks_data.csv' 파일로 저장되었습니다.")