# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a market data pipeline project (market-data-pipeline) focused on collecting historical stock data and implementing daily data batch pipelines. The project description is in Korean: "주식의 과거 데이터 수집, 그리고 일간 데이터 배치 파이프라인".

## Current State

This repository is currently in its initial state with only basic documentation files:
- README.md: Contains project description in Korean
- LICENSE: MIT license

## Development Setup

**IMPORTANT: Python Execution Rule**
- ALWAYS use `uv run python` to execute Python scripts in this project
- Never use plain `python` command
- This ensures proper dependency management and environment isolation

**IMPORTANT: Data Processing Library Rule**
- ALWAYS use `polars` for data processing and manipulation
- Never use `pandas` for new code
- Polars provides better performance and memory efficiency

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