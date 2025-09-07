# Market Data Pipeline

주식의 과거 데이터 수집, 그리고 일간 데이터 배치 파이프라인

## Setup

### 1. Environment Setup

Copy the environment template and configure your settings:
```bash
cp .env.example .env
# Edit .env with your actual configuration
```

### 2. Database Setup

Start PostgreSQL using Docker Compose:
```bash
docker-compose up -d
```

### 3. Install Dependencies

```bash
uv sync
```

### 4. Load Initial Data

```bash
uv run python load_delisted_data.py
```

## Development

### Python Execution
- Always use `uv run python` to execute Python scripts
- Use `polars` for data processing (not pandas)

### Database Access
- PostgreSQL runs on `localhost:5432`
- Database: `market_data`
- Use DataGrip or similar tools for data verification

## Project Structure

```
src/
├── database/          # Database connection and models
├── models/           # SQLAlchemy models
└── ...

scripts/              # Data loading and processing scripts
```

## License

MIT License
