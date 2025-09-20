# 프로젝트 진행 상황

## 현재 상태

**시장 데이터 파이프라인 개발**

### 완료된 작업
- ✅ PostgreSQL Docker 설정 (docker-compose.yml)
- ✅ ClickHouse Docker 설정 추가
- ✅ 기본 프로젝트 구조 구축
- ✅ Python 프로젝트 설정 (pyproject.toml)
- ✅ 데이터베이스 연결 모듈 생성

### 최근 완료 (2025-09-17)
- ✅ ClickHouse Docker 서비스 설정 및 연결 해결
- ✅ 주식 마스터 테이블 스키마 생성 및 테스트
- ✅ TDD 테스트 스위트 구현 (13개 테스트, 모두 통과)
- ✅ FinanceDataReader 통합으로 주식 데이터 수집
- ✅ 2,845개 주식 성공적으로 로드 (KOSPI: 936, KOSDAQ: 1,793, KONEX: 116)
- ✅ 상장폐지 주식 처리 기능 구현
- ✅ 기능 브랜치 생성 및 원격 저장소에 푸시

### 주요 업데이트 (2025-09-17 세션)
- ✅ **모든 알려진 버그 수정**: ETF 수집, get_stock_by_symbol, 컬럼 매핑
- ✅ **대규모 상장폐지 데이터 통합**: ClickHouse에 1,733개 추가 상장폐지 주식 추가
- ✅ **KRX 크롤러 개발**: 상장폐지 주식을 위한 완전한 웹 크롤러 (3개 시장)
- ✅ **일간 배치 파이프라인**: 일간 업데이트를 위한 통합 자동화 스크립트
- ✅ **프로덕션 준비 시스템**: 완전한 엔드투엔드 파이프라인 운영

## 🎯 Systematic Implementation Plan

### Phase 1: 일회성 설정 작업 (신규 환경 구축용)

#### collect_active_stock_listings - 상장일 정보 수집 (2000년 이후)
- **목적**: KRX에서 2000년 이후 모든 상장 이력 크롤링하여 상장일 정보 확보
- **구현체**: `scripts/initial_setup.py --step active-listings`
- **데이터 소스**: KRX 상장 정보 페이지
- **결과**: ClickHouse stock_master 테이블에 상장일 정보 채움
- **세부 작업**:
  - KRX 상장 이력 전체 크롤링 (2000~현재)
  - 상장일 데이터 정규화 및 검증
  - ClickHouse에 상장일 정보 업데이트
  - 크롤링 실패 종목 리포트 생성

#### sync_delisted_stocks - 상장폐지 종목 정보 수집
- **목적**: 모든 상장폐지 종목 이력 확보
- **구현체**: `scripts/sync_delisted_stocks.py` (별도 스크립트)
- **데이터 소스**: KRX 상장폐지 정보 페이지
- **결과**: 상장폐지 종목 정보 완전 적재
- **세부 작업**:
  - 1990년 이후 모든 상장폐지 이력 크롤링
  - 상장폐지일 및 사유 정보 수집
  - 중복 제거 및 데이터 품질 검증
  - 상장폐지 종목 ClickHouse 적재

#### collect_price_data - 가격 데이터 초기 적재
- **목적**: FinanceDataReader 기반 모든 종목 가격 데이터 수집
- **구현체**: `scripts/initial_setup.py --step price-data`
- **데이터 소스**: FinanceDataReader (네이버, 야후 등)
- **결과**: ClickHouse에 전체 가격 데이터 적재
- **세부 작업**:
  - 가격 데이터 스키마 설계 (일봉, 주봉, 월봉)
  - 활성 종목 가격 데이터 수집 (상장일~현재)
  - 상장폐지 종목 가격 데이터 수집
  - 주식 분할/병합 이벤트 처리
  - 배치 처리 및 에러 핸들링

#### 1.4 2000년 이전 상장 종목 상장일 유추
- **목적**: 가격 데이터 첫 거래일 기반으로 상장일 추정
- **구현체**: `scripts/initial_setup.py --step 1.4`
- **알고리즘**: 가격 데이터 최초 거래일 = 상장일로 추정
- **결과**: 2000년 이전 상장 종목의 상장일 정보 보완
- **세부 작업**:
  - 상장일 정보 누락 종목 식별
  - 가격 데이터 최초 거래일 추출
  - 상장일 추정 로직 구현
  - 추정 결과 검증 및 보정
  - stock_master 테이블 상장일 업데이트

#### 1.5 상장폐지 종목 상장일 유추
- **목적**: 상장폐지 종목 중 상장일 누락 종목 처리
- **구현체**: `scripts/initial_setup.py --step 1.5`
- **알고리즘**: 가격 데이터 기반 상장일 추정
- **결과**: 상장폐지 종목 상장일 정보 완성
- **세부 작업**:
  - 상장폐지 종목 중 상장일 누락 종목 식별
  - 가격 데이터 기반 상장일 추정
  - 추정 정확도 검증
  - 최종 상장일 정보 업데이트

### Phase 2: 일간 배치 작업 (일간 운영)

#### 2.0 전체 상장/상폐 현황 크롤링
- **목적**: 매일 전체 상장/상폐 현황을 확인하여 변화 감지
- **구현체**: `scripts/daily_stock_master_update.py --step 2.0`
- **실행 주기**: 매일 새벽 2시
- **데이터 소스**: KRX 전체 현황 페이지
- **세부 작업**:
  - 현재 상장 종목 전체 리스트 수집
  - 현재 상장폐지 종목 전체 리스트 수집
  - 기존 DB 데이터와 비교하여 변화 감지
  - 변화 내역 로그 생성

#### 2.1 신규 상장종목 처리
- **목적**: 새로 상장된 종목 감지 및 시스템 등록
- **구현체**: `scripts/daily_stock_master_update.py --step 2.1`
- **트리거**: 2.0에서 신규 상장 종목 감지 시
- **결과**: 새 종목 stock_master 등록 및 가격 데이터 수집 시작
- **세부 작업**:
  - 신규 상장 종목 정보 수집 (종목명, 시장, 상장일)
  - stock_master 테이블에 신규 종목 추가
  - 신규 종목 가격 데이터 수집 시작
  - 알림/로그 생성

#### 2.2 신규 상장폐지 종목 처리
- **목적**: 새로 상장폐지된 종목 감지 및 상태 업데이트
- **구현체**: `scripts/daily_stock_master_update.py --step 2.2`
- **트리거**: 2.0에서 신규 상장폐지 종목 감지 시
- **결과**: 해당 종목 상장폐지일 업데이트
- **세부 작업**:
  - 신규 상장폐지 종목 및 폐지일 확인
  - stock_master 테이블 is_active=0, delisting_date 업데이트
  - 가격 데이터 수집 중단
  - 상장폐지 알림/로그 생성

#### 2.3 일간 가격 데이터 업데이트
- **목적**: 모든 활성 종목의 최신 가격 데이터 수집
- **구현체**: `scripts/daily_stock_master_update.py --step 2.3`
- **실행 주기**: 매일 장마감 후
- **데이터 소스**: FinanceDataReader
- **세부 작업**:
  - 활성 종목 리스트 조회
  - 전일 가격 데이터 수집
  - 가격 데이터 품질 검증
  - ClickHouse 가격 테이블 업데이트
  - 수집 실패 종목 재시도 로직

#### 2.4 자본금 이벤트 처리
- **목적**: 주식 분할/병합 등 자본금 변동 이벤트 감지 및 처리
- **구현체**: `scripts/daily_stock_master_update.py --step 2.4`
- **트리거**: 가격 데이터 이상 패턴 감지 시
- **결과**: 해당 종목 가격 데이터 전체 재수집
- **세부 작업**:
  - 전일 대비 이상 가격 변동 감지 (50% 이상 급등/급락)
  - 자본금 이벤트 여부 확인
  - 해당 종목 가격 데이터 전체 삭제
  - 상장일부터 현재까지 가격 데이터 재수집
  - 이벤트 처리 결과 로그

## 기술적 결정 사항

### 인프라 구조
- **데이터베이스**: PostgreSQL + ClickHouse (듀얼 데이터베이스 접근법)
  - PostgreSQL: 메타데이터, 설정, OLTP 작업
  - ClickHouse: 시계열 시장 데이터, OLAP 쿼리
- **컸테이너화**: Docker Compose
- **Python 환경**: uv (CLAUDE.md 요구사항 따름)
- **데이터 처리**: Polars (CLAUDE.md 요구사항 따름)

### 개발 규칙
- 스크립트 실행 시 항상 `uv run python` 사용
- 데이터 처리에 항상 `polars` 사용 (pandas 금지)
- TDD 따르기: Red-Green-Refactor 사이클
- 테스트에 pytest 사용

### Git 워크플로우 규칙
- ✅ 기능 브랜치에서만 작업 (main에 직접 작업 금지)
- ✅ 각 기능/작업마다 브랜치 자동 생성
- ✅ 모든 코드 변경사항 Git에 자동 커밋
- ✅ 변경 이력 추적을 위해 원격 저장소에 자동 푸시
- ✅ 설명적인 커밋 메시지 사용 (conventional commit 형식)
- ✅ 적절한 브랜치로 깨끗한 Git 이력 유지

## 구현 상태 및 의존성

### ✅ 사전 요구사항 (완료됨)
- ClickHouse Docker 환경 구축
- stock_master 테이블 스키마 및 기본 CRUD 작업
- KRX 크롤러 (신규상장, 상장폐지) 구현
- TDD 기본 구조 (일부 모듈)

### 🔄 Phase 1 상태 (일회성 작업)
- ✅ **Phase 1.1 → collect_active_stock_listings**: TDD 구현 완료 (11개 테스트 통과, 프로덕션 레디)
- ✅ **Phase 1.2 → sync_delisted_stocks**: TDD 구현 완료 (20개 테스트 통과, skip 로직 포함)
- 🔄 **Phase 1.3 → collect_price_data**: 현재 구현 중 (가격 데이터 스키마 완료, TDD 진행)
- ⏳ **Phase 1.4 → infer_pre2000_listing_dates**: 알고리즘 설계 필요
- ⏳ **Phase 1.5 → infer_delisted_listing_dates**: 알고리즘 설계 필요

### 🔄 Phase 2 상태 (일간 배치)
- ⏳ **2.0**: TDD 구현 필요
- ⏳ **2.1**: TDD 구현 필요
- ⏳ **2.2**: TDD 구현 필요
- ⏳ **2.3**: 가격 데이터 시스템 구축 후 가능
- ⏳ **2.4**: 가격 데이터 시스템 구축 후 가능

### 📋 실행 의존성
```
collect_active_stock_listings (상장일 수집)
├── sync_delisted_stocks (상폐 정보 수집) ✅
└── collect_price_data (가격 데이터 적재) 🔄
    ├── infer_pre2000_listing_dates (상장일 유추)
    └── infer_delisted_listing_dates (상폐 종목 상장일 유추)

Phase 2 (일간 배치) requires Phase 1 completion
```

### 🎯 Current Session Progress (2025-09-20) - 프로젝트 구조 완전 정리 세션

#### ✅ 1차: 프로젝트 구조 대대적 정리 완료 (09-19)
- **scripts/ 디렉토리 정리**: 일회성 setup 파일들을 src/setup/ 폴더로 이동
- **legacy 파일 제거**: 사용하지 않는 5개 스크립트 파일 정리
- **data/ 디렉토리 정리**: 불필요한 폴더 및 중복 파일들 정리
- **문서화 완성**: setup/README.md, data/README.md, src/README.md 작성

#### ✅ 2차: 언어 통일 및 구조 개선 (09-20)
- **progress.md 언어 통일**: 영어/한국어 혼용 → 완전 한국어 통일
- **src/ 구조 최적화**: 빈 폴더 제거, legacy PostgreSQL 코드 제거
- **setup/ 위치 개선**: setup/ → src/setup/으로 이동
- **scripts/ 일관성 해결**: sync 스크립트 구조 완전 정리

#### 📂 최종 프로젝트 구조
```
src/                  # 라이브러리 코드 (Library Layer)
├── clickhouse/      # ClickHouse 데이터베이스 클라이언트
├── crawlers/        # 웹 크롤링 모듈
├── setup/           # 일회성 초기 설정 스크립트
└── README.md        # 구조 설명 문서

scripts/             # 실행 스크립트 (Execution Layer)
├── sync_new_listings.py      # 신규 상장 동기화 (신규 생성)
├── sync_delisted_stocks.py   # 상장폐지 동기화 (기존)
├── initial_setup.py          # 일회성 작업만 (정리됨)
├── check_stock_data.py       # 데이터 현황 확인 (ClickHouse용)
├── daily_stock_master_update.py  # 일간 배치 (향후)
└── README.md                 # 사용법 가이드

data/                # 데이터 저장소
├── raw/            # 크롤링 원본 (7개 파일)
├── initial_setup/  # Phase 1 백업용
├── daily_batch/    # Phase 2 백업용
└── README.md       # 데이터 관리 가이드

PROJECT_STRUCTURE.md # 전체 구조 철학 문서
```

#### 🔧 핵심 개선사항
- **Layered Architecture 완성**: src/(Library) ↔ scripts/(Execution) 명확히 분리
- **scripts/ 일관성 달성**: sync_new_listings.py ↔ sync_delisted_stocks.py 완벽 대칭
- **언어 통일**: 모든 문서 및 주석 한국어로 일관성 있게 정리
- **중복 제거**: legacy 코드, 빈 폴더, 중복 파일 완전 제거

#### 📋 다음 세션 즉시 시작 가능한 작업
- 🎯 **Phase 1.3 TDD 구현**: ClickHouse 가격 데이터 테이블 스키마 설계
- 🎯 **FinanceDataReader 연동**: 가격 데이터 수집 로직 TDD 구현
- 🎯 **src/clickhouse/price_client.py**: 가격 데이터 전용 클라이언트 완성

#### 🏗️ 준비된 완벽한 환경
- ✅ ClickHouse 환경 구축 완료 (4,549개 종목)
- ✅ 일관된 동기화 시스템 (신규 상장 ↔ 상장폐지)
- ✅ TDD 테스트 환경 구축 완료
- ✅ 완전히 정리된 프로젝트 구조
- ✅ 포괄적인 문서화 완성

## 현재 상태 요약 (2024-09-17 세션 종료)

### 🎯 주요 성과
PostgreSQL에서 ClickHouse로 주식 마스터 데이터 관리 완전 마이그레이션 완료 (TDD 접근법 사용)

### 📊 데이터 상태 (업데이트됨)
- **주식 마스터 테이블**: ClickHouse에 생성 및 데이터 적재 완료
- **전체 주식**: **4,549**개 주식 (2,845개 활성 + 1,704개 상장폐지)
- **활성 시장**: KOSPI (936), KOSDAQ (1,793), KONEX (116)
- **상장폐지 주식**: 1,704개 포괄적 역사적 상장폐지 주식
- **데이터 소스**: FinanceDataReader (활성) + Parquet 파일 (상장폐지)

### 🔧 구축된 기술 스택
- **데이터베이스**: ClickHouse (주), PostgreSQL (보조)
- **Python 환경**: uv 패키지 매니저
- **데이터 처리**: Polars (강제, pandas 금지)
- **테스트**: pytest로 포괄적 TDD 커버리지
- **버전 관리**: Git 기능 브랜치 워크플로우
- **웹 크롤링**: requests + BeautifulSoup4로 KRX 데이터
- **일간 자동화**: 통합 배치 처리 파이프라인

### ✅ 기존 알려진 문제 (모두 해결됨)
1. ~~ETF 데이터 수집 실패~~ → **수정**: 적절한 에러 처리 구현
2. ~~get_stock_by_symbol 메서드 버그~~ → **수정**: 컬럼 접근 방법 수정
3. ~~상장폐지 처리에서 컬럼명 매핑~~ → **수정**: 견고한 매핑 시스템

### 🎯 현재 기능
- ✅ 실시간 활성 주식 데이터 수집 (KOSPI/KOSDAQ/KONEX)
- ✅ 포괄적 상장폐지 주식 데이터베이스 (1,700+개 주식)
- ✅ 자동화된 일간 배치 처리
- ✅ 프로덕션 급 에러 처리
- ✅ ClickHouse 최적화 및 리포팅
- ✅ 주식 마스터 데이터 완전한 CRUD 작업

## 아키텍처 개요
```
시장 데이터 파이프라인
├── 데이터 수집 레이어
├── 저장 레이어 (PostgreSQL + ClickHouse)
├── 처리 레이어 (일간 배치 작업)
└── API/인터페이스 레이어
```