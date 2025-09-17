# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a market data pipeline project (market-data-pipeline) focused on collecting historical stock data and implementing daily data batch pipelines. The project description is in Korean: "주식의 과거 데이터 수집, 그리고 일간 데이터 배치 파이프라인".

## Current State

**IMPORTANT: Always check progress.md for the latest project status and current work state.**

The project has evolved beyond initial setup. See `progress.md` for detailed current status, completed tasks, and next steps. Key current state:
- PostgreSQL and ClickHouse Docker setup completed
- Python project structure established
- Database connection modules implemented
- Active development in progress

## Development Setup

**IMPORTANT: Python Execution Rule**
- ALWAYS use `uv run python` to execute Python scripts in this project
- Never use plain `python` command
- This ensures proper dependency management and environment isolation

**IMPORTANT: Data Processing Library Rule**
- ALWAYS use `polars` for data processing and manipulation
- Never use `pandas` for new code
- Polars provides better performance and memory efficiency

**IMPORTANT: Test-Driven Development (TDD) Rule**
- ALWAYS follow TDD principles when implementing new features
- Write tests FIRST, then implement the functionality
- Follow the Red-Green-Refactor cycle:
  1. Red: Write a failing test
  2. Green: Write minimal code to make the test pass
  3. Refactor: Improve code while keeping tests passing
- All new code must have corresponding tests before implementation
- Use pytest as the testing framework

**IMPORTANT: Git Workflow Rules**
- ALWAYS work on feature branches, never directly on main
- Automatically create new branches for each feature/task
- ALL code changes must be automatically committed to Git
- ALWAYS push changes to remote repository for change history tracking
- Use descriptive commit messages following conventional commit format
- Maintain clean Git history with proper branching strategy

### For Python-based pipeline:
- Set up virtual environment
- Add requirements.txt or pyproject.toml
- Establish testing framework (pytest recommended)
- Add linting/formatting tools (black, flake8/ruff)

### For Node.js-based pipeline:
- Initialize package.json
- Set up testing framework (Jest recommended)
- Add linting/formatting tools (ESLint, Prettier)

### For other technology stacks:
- Add appropriate dependency management files
- Establish testing and linting commands

## Architecture

The architecture is not yet established. When implementing the market data pipeline, consider:
- Data collection components for historical stock data
- Batch processing pipeline for daily data updates
- Storage layer for collected data
- Scheduling/orchestration for batch operations

## License

This project is licensed under the MIT License.