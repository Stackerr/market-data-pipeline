# Source Code Structure

이 디렉토리는 Market Data Pipeline의 핵심 소스 코드를 포함합니다.

## 📁 모듈 구조

### `/clickhouse/`
**목적**: ClickHouse 데이터베이스 관련 클라이언트 모듈

**포함 파일**:
- `client.py`: 기본 ClickHouse HTTP 클라이언트
- `stock_master.py`: 주식 마스터 데이터 관리 클라이언트
- `price_client.py`: 주식 가격 데이터 전용 클라이언트

**역할**:
- ClickHouse 연결 및 쿼리 실행
- 데이터 CRUD 작업
- 스키마 관리 및 최적화

---

### `/crawlers/`
**목적**: 외부 데이터 소스 크롤링 모듈

**포함 파일**:
- `krx_new_listing_crawler.py`: KRX 신규 상장 종목 크롤러
- `krx_delisted_crawler.py`: KRX 상장폐지 종목 크롤러

**역할**:
- KRX 웹사이트 데이터 크롤링
- HTML 파싱 및 데이터 정규화
- 에러 핸들링 및 재시도 로직

---

### `/setup/`
**목적**: 프로젝트 초기 환경 구축 스크립트

**포함 파일**:
- `setup_clickhouse.py`: ClickHouse 데이터베이스 초기화
- `setup_stock_master_clickhouse.py`: stock_master 테이블 생성
- `load_stock_master_clickhouse.py`: 초기 주식 데이터 적재
- `README.md`: 설정 방법 상세 가이드

**사용법**:
```python
# 프로젝트 내에서 import
from src.setup.setup_clickhouse import setup_database

# 또는 직접 실행
uv run python src/setup/setup_clickhouse.py
```

## 🎯 설계 원칙

### 1. 단일 책임 원칙
- 각 모듈은 명확한 단일 목적을 가짐
- ClickHouse 관련, 크롤링 관련, 설정 관련으로 명확히 분리

### 2. 의존성 최소화
- 모듈 간 순환 의존성 없음
- 외부 의존성을 명시적으로 관리

### 3. 확장성 고려
- 새로운 데이터 소스 추가 시 `/crawlers/`에 확장
- 새로운 데이터베이스 추가 시 별도 모듈로 확장 가능

## 🔄 Import 경로

프로젝트 루트에서 다음과 같이 import:

```python
# ClickHouse 관련
from src.clickhouse.stock_master import ClickHouseStockMaster
from src.clickhouse.price_client import ClickHousePriceClient

# 크롤러 관련
from src.crawlers.krx_new_listing_crawler import KRXNewListingCrawler
from src.crawlers.krx_delisted_crawler import KRXDelistedCrawler

# 설정 관련
from src.setup.setup_clickhouse import setup_database
```

## 📊 모듈 의존성

```
scripts/           # 실행 스크립트들
├── initial_setup.py
├── sync_delisted_stocks.py
└── daily_stock_master_update.py
    ↓ import
src/               # 핵심 라이브러리 코드
├── clickhouse/   # 데이터베이스 레이어
├── crawlers/     # 데이터 수집 레이어
└── setup/        # 초기화 레이어
```

## ⚠️ 주의사항

1. **모듈 순수성**: src/ 내 코드는 외부 파일 생성/수정 최소화
2. **테스트 가능성**: 모든 모듈은 단위 테스트 가능하도록 설계
3. **환경 독립성**: 하드코딩된 경로나 설정 지양

---
Last Updated: 2025-09-19