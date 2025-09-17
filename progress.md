# Project Progress

## Current Status

**Market Data Pipeline Development**

### Completed
- ✅ PostgreSQL Docker setup (docker-compose.yml)
- ✅ ClickHouse Docker configuration added
- ✅ Basic project structure established
- ✅ Python project configuration (pyproject.toml)
- ✅ Database connection module created (src/database/connection.py)

### In Progress
- 🔄 ClickHouse Docker service testing (port conflict needs resolution)

### Next Steps
- 📋 Resolve ClickHouse port conflict with existing container
- 📋 Test ClickHouse connectivity
- 📋 Implement stock master data loading script
- 📋 Set up daily batch pipeline architecture
- 📋 Implement TDD approach for new features

## Technical Decisions

### Infrastructure
- **Database**: PostgreSQL + ClickHouse (dual database approach)
  - PostgreSQL: Metadata, configuration, OLTP operations
  - ClickHouse: Time-series market data, OLAP queries
- **Containerization**: Docker Compose
- **Python Environment**: uv (as per CLAUDE.md requirements)
- **Data Processing**: Polars (as per CLAUDE.md requirements)

### Development Rules
- Always use `uv run python` for script execution
- Always use `polars` for data processing (no pandas)
- Follow TDD: Red-Green-Refactor cycle
- Use pytest for testing

### Git Workflow Rules (NEW)
- ✅ Work on feature branches only (never directly on main)
- ✅ Auto-create branches for each feature/task
- ✅ Auto-commit ALL code changes to Git
- ✅ Auto-push to remote repository for change history tracking
- ✅ Use descriptive commit messages (conventional commit format)
- ✅ Maintain clean Git history with proper branching

## Current Challenges
1. Port 8123 already in use by existing ClickHouse container
2. Need to coordinate with existing ClickHouse setup

## Architecture Overview
```
Market Data Pipeline
├── Data Collection Layer
├── Storage Layer (PostgreSQL + ClickHouse)
├── Processing Layer (Daily Batch Jobs)
└── API/Interface Layer
```