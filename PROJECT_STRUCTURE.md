# 프로젝트 구조 설명

## 📁 디렉토리 구조 개요

```
market-data-pipeline/
├── src/                    # 라이브러리 코드 (Library Layer)
│   ├── clickhouse/        # ClickHouse 데이터베이스 클라이언트
│   ├── crawlers/          # 웹 크롤링 모듈
│   └── setup/             # 일회성 초기 설정 스크립트
├── scripts/               # 실행 스크립트 (Execution Layer)
├── data/                  # 데이터 저장소
├── tests/                 # 테스트 코드
└── docs/                  # 문서
```

## 🎯 설계 철학: Layered Architecture

### Layer 1: `src/` - Library Layer (라이브러리 레이어)
**역할**: 재사용 가능한 비즈니스 로직과 핵심 기능

**특징**:
- 순수 Python 클래스와 함수들
- 외부 의존성 최소화
- 단위 테스트 가능
- 다른 프로젝트에서도 재사용 가능

**모듈 구성**:
- `clickhouse/`: ClickHouse 데이터베이스 연결 및 CRUD
- `crawlers/`: KRX 웹사이트 크롤링 로직
- `setup/`: 초기 환경 설정 유틸리티

### Layer 2: `scripts/` - Execution Layer (실행 레이어)
**역할**: src/ 모듈을 조합해서 실제 작업을 수행하는 CLI 스크립트

**특징**:
- `src/` 모듈들을 import해서 사용
- 명령행 인터페이스 제공
- 사용자와의 상호작용 처리
- 로깅 및 에러 처리

**스크립트 구성**:
- `initial_setup.py`: Phase 1 초기화 작업
- `sync_delisted_stocks.py`: 상장폐지 데이터 동기화
- `daily_stock_master_update.py`: 일간 배치 작업
- `check_stock_data.py`: 데이터 현황 확인
- `check_new_listings.py`: 신규 상장 확인

## 🔄 의존성 흐름

```
scripts/               # CLI Interface
    ↓ import
src/                   # Core Library
    ↓ connect
ClickHouse Database    # Data Storage
```

**예시**:
```python
# scripts/sync_delisted_stocks.py
from src.crawlers.krx_delisted_crawler import KRXDelistedCrawler
from src.clickhouse.stock_master import ClickHouseStockMaster

# src/ 모듈들을 조합해서 실제 작업 수행
crawler = KRXDelistedCrawler()
stock_master = ClickHouseStockMaster()
data = crawler.crawl_all_markets_full_sync()
stock_master.insert_stocks(data)
```

## ✅ 이 구조의 장점

### 1. 관심사 분리 (Separation of Concerns)
- **src/**: "어떻게" (How) - 비즈니스 로직
- **scripts/**: "무엇을" (What) - 실행 흐름

### 2. 재사용성 (Reusability)
- src/ 모듈은 다양한 scripts에서 재사용 가능
- 새로운 스크립트 추가 시 기존 로직 재활용

### 3. 테스트 용이성 (Testability)
- src/ 모듈들은 독립적으로 단위 테스트 가능
- scripts는 통합 테스트로 검증

### 4. 유지보수성 (Maintainability)
- 비즈니스 로직 변경 시 src/만 수정
- CLI 인터페이스 변경 시 scripts/만 수정

## 🚫 잘못된 접근법 vs ✅ 올바른 접근법

### ❌ 잘못된 방법: 모든 코드를 scripts/에
```python
# scripts/bad_example.py
class ClickHouseClient:  # 비즈니스 로직이 script에
    def connect(self): ...
    def query(self): ...

def crawl_data():  # 크롤링 로직도 script에
    # 복잡한 크롤링 로직...
    pass

if __name__ == "__main__":
    # 실행 로직
    crawl_data()
```

### ✅ 올바른 방법: 레이어 분리
```python
# src/clickhouse/client.py
class ClickHouseClient:  # 비즈니스 로직은 src/에
    def connect(self): ...
    def query(self): ...

# src/crawlers/krx_crawler.py
class KRXCrawler:  # 크롤링 로직도 src/에
    def crawl_data(self): ...

# scripts/data_collection.py
from src.clickhouse.client import ClickHouseClient
from src.crawlers.krx_crawler import KRXCrawler

if __name__ == "__main__":  # 실행 로직만 scripts/에
    crawler = KRXCrawler()
    client = ClickHouseClient()
    # 모듈들을 조합해서 작업 수행
```

## 📋 추가 개선 사항

### 1. 공통 유틸리티 모듈
현재 모든 scripts에 중복되는 코드:
```python
# 모든 script에 반복됨
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
```

**개선안**: `src/utils/common.py` 생성
```python
# src/utils/common.py
def setup_project_path():
    """프로젝트 루트를 Python 경로에 추가"""
    # 공통 로직
```

### 2. 설정 관리 중앙화
**개선안**: `src/config/` 모듈로 환경 설정 통합 관리

## 🎯 결론

현재 `scripts/`와 `src/`의 분리는 **올바른 설계**입니다:

- **src/**: 재사용 가능한 라이브러리 코드
- **scripts/**: src/를 사용하는 실행 스크립트

이 구조를 유지하면서 중복 코드 제거와 공통 유틸리티 추가로 더욱 개선할 수 있습니다.

---
Last Updated: 2025-09-19