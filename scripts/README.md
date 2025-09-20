# Scripts Directory

이 디렉토리는 Market Data Pipeline의 실행 스크립트들을 포함합니다.

## 📁 스크립트 목록

### 🔄 동기화 스크립트 (Sync Scripts)

#### `sync_new_listings.py`
**목적**: 신규 상장 종목 동기화 시스템

**기능**:
- KRX에서 2000년 이후 모든 신규 상장 이력 크롤링
- 상장일 데이터 정규화 및 검증
- ClickHouse에 상장일 정보 업데이트
- skip 로직으로 중복 처리 방지

**사용법**:
```bash
# 기본 실행 (2000년 이후, skip 로직 활성화)
uv run python scripts/sync_new_listings.py

# 특정 연도부터 시작
uv run python scripts/sync_new_listings.py --start-year 2010

# skip 로직 비활성화 (기존 데이터도 재처리)
uv run python scripts/sync_new_listings.py --no-skip

# 상태 확인만
uv run python scripts/sync_new_listings.py --status
```

---

#### `sync_delisted_stocks.py`
**목적**: 상장폐지 종목 동기화 시스템

**기능**:
- KRX에서 1990년 이후 모든 상장폐지 이력 크롤링
- 상장폐지일 및 사유 정보 수집
- 중복 제거 및 데이터 품질 검증
- 상장폐지 종목 ClickHouse 적재

**사용법**:
```bash
# 기본 실행
uv run python scripts/sync_delisted_stocks.py

# 특정 연도부터 시작
uv run python scripts/sync_delisted_stocks.py --start-year 2000

# skip 로직 비활성화
uv run python scripts/sync_delisted_stocks.py --no-skip
```

---

### 🔧 초기화 및 유틸리티 스크립트

#### `initial_setup.py`
**목적**: Phase 1 일회성 초기 설정 작업

**기능**:
- 가격 데이터 초기 적재 (Phase 1.3)
- 2000년 이전 상장일 추정 (Phase 1.4)
- 상장폐지 종목 상장일 추정 (Phase 1.5)

**사용법**:
```bash
# 가격 데이터 초기 적재
uv run python scripts/initial_setup.py --step price-data

# 2000년 이전 상장일 추정
uv run python scripts/initial_setup.py --step infer-pre2000

# 상장폐지 종목 상장일 추정
uv run python scripts/initial_setup.py --step infer-delisted
```

**Note**: 신규 상장 및 상장폐지 데이터는 별도 동기화 스크립트 사용

---

#### `check_stock_data.py`
**목적**: ClickHouse 주식 마스터 데이터 현황 확인

**기능**:
- 전체 종목 수 및 시장별 현황 조회
- 활성/상장폐지 종목 샘플 확인
- 데이터 품질 현황 점검

**사용법**:
```bash
uv run python scripts/check_stock_data.py
```

---

#### `daily_stock_master_update.py`
**목적**: Phase 2 일간 배치 작업 (미완성)

**기능** (계획):
- 일간 상장/상폐 현황 크롤링
- 신규 상장종목 자동 처리
- 신규 상장폐지 종목 자동 처리
- 일간 가격 데이터 업데이트

**사용법** (향후):
```bash
uv run python scripts/daily_stock_master_update.py --step 2.0
```

## 🎯 스크립트 사용 순서

### 신규 환경 구축 시:
```bash
# 1. 초기 환경 설정 (src/setup/ 스크립트)
uv run python src/setup/setup_clickhouse.py
uv run python src/setup/setup_stock_master_clickhouse.py
uv run python src/setup/load_stock_master_clickhouse.py

# 2. 신규 상장 데이터 동기화
uv run python scripts/sync_new_listings.py

# 3. 상장폐지 데이터 동기화
uv run python scripts/sync_delisted_stocks.py

# 4. 가격 데이터 초기 적재 (Phase 1.3 완성 후)
uv run python scripts/initial_setup.py --step price-data

# 5. 데이터 현황 확인
uv run python scripts/check_stock_data.py
```

### 일상 운영 시:
```bash
# 정기적으로 신규 상장/상폐 데이터 동기화
uv run python scripts/sync_new_listings.py
uv run python scripts/sync_delisted_stocks.py

# 데이터 상태 확인
uv run python scripts/check_stock_data.py
```

## 🔄 설계 원칙

### 1. 일관성 (Consistency)
- 동기화 스크립트는 모두 `sync_*.py` 패턴
- 모든 스크립트는 동일한 CLI 인터페이스 (`--start-year`, `--no-skip` 등)
- 동일한 로깅 포맷과 에러 처리

### 2. 단일 책임 (Single Responsibility)
- 각 스크립트는 하나의 명확한 목적
- `sync_new_listings.py`: 신규 상장만 처리
- `sync_delisted_stocks.py`: 상장폐지만 처리
- `initial_setup.py`: 일회성 작업만 처리

### 3. 재사용성 (Reusability)
- 모든 스크립트는 `src/` 모듈을 import해서 사용
- 비즈니스 로직은 `src/`에, 실행 로직만 `scripts/`에

### 4. 독립성 (Independence)
- 각 스크립트는 독립적으로 실행 가능
- 의존성 최소화
- 상태 확인 기능 내장

## ⚠️ 주의사항

1. **실행 환경**: 모든 스크립트는 프로젝트 루트에서 `uv run python` 사용
2. **데이터 백업**: 동기화 스크립트는 자동으로 백업 생성
3. **skip 로직**: 기본적으로 중복 처리 방지, 필요시 `--no-skip` 사용
4. **에러 처리**: 모든 스크립트는 적절한 exit code 반환

---
Last Updated: 2025-09-19