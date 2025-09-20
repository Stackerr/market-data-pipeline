# Setup Scripts Documentation

이 디렉토리는 프로젝트 초기 환경 구축을 위한 일회성 스크립트들을 포함합니다.

## 📁 파일 목록

### 1. setup_clickhouse.py
**목적**: ClickHouse Docker 컨테이너 초기 설정 및 데이터베이스 생성

**사용법**:
```bash
uv run python src/setup/setup_clickhouse.py
```

**기능**:
- ClickHouse Docker 컨테이너 상태 확인
- `market_data` 데이터베이스 생성
- 기본 연결 테스트

**실행 조건**: Docker가 실행 중이고 ClickHouse 컨테이너가 시작된 상태

---

### 2. setup_stock_master_clickhouse.py
**목적**: ClickHouse에 stock_master 테이블 스키마 생성

**사용법**:
```bash
uv run python src/setup/setup_stock_master_clickhouse.py
```

**기능**:
- `stock_master` 테이블 스키마 생성
- 필수 인덱스 설정
- 테이블 구조 검증

**의존성**: setup_clickhouse.py 선행 실행 필요

---

### 3. load_stock_master_clickhouse.py
**목적**: FinanceDataReader를 통한 초기 주식 마스터 데이터 적재

**사용법**:
```bash
uv run python src/setup/load_stock_master_clickhouse.py
```

**기능**:
- KOSPI, KOSDAQ, KONEX 활성 종목 데이터 수집
- ClickHouse stock_master 테이블에 초기 데이터 적재
- 데이터 품질 검증 및 통계 리포트

**의존성**: setup_stock_master_clickhouse.py 선행 실행 필요

## 🚀 초기 설정 실행 순서

새로운 환경에서 프로젝트를 시작할 때 다음 순서로 실행:

```bash
# 1. Docker 환경 시작
docker-compose up -d

# 2. ClickHouse 데이터베이스 초기화
uv run python src/setup/setup_clickhouse.py

# 3. stock_master 테이블 생성
uv run python src/setup/setup_stock_master_clickhouse.py

# 4. 초기 주식 데이터 적재
uv run python src/setup/load_stock_master_clickhouse.py
```

## ⚠️ 주의사항

1. **일회성 스크립트**: 각 스크립트는 환경 구축 시 한 번만 실행하면 됩니다.
2. **실행 순서**: 위 순서를 반드시 지켜야 합니다.
3. **재실행**: 이미 설정된 환경에서 재실행 시 데이터 중복이나 오류가 발생할 수 있습니다.
4. **환경 의존성**: Docker 및 ClickHouse 컨테이너가 정상 동작해야 합니다.

## 📊 실행 결과 확인

설정 완료 후 다음 명령어로 상태 확인:

```bash
# ClickHouse 연결 테스트
uv run python -c "
from src.clickhouse.stock_master import ClickHouseStockMaster
sm = ClickHouseStockMaster()
print('📊 Stock count:', sm.get_stock_count())
"
```

정상적으로 설정되었다면 주식 데이터 개수가 출력됩니다.

## 🔄 일상 운영

초기 설정 완료 후 일상 운영에는 다음 스크립트들을 사용:
- `scripts/initial_setup.py`: Phase 1 초기화 작업
- `scripts/sync_delisted_stocks.py`: 상장폐지 데이터 동기화
- `scripts/daily_stock_master_update.py`: 일간 배치 작업

---
Last Updated: 2025-09-19