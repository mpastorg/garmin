# Garmin Data Extraction Project - AI Agent Instructions

## Overview
This is a Python script that connects to Garmin Connect API to fetch daily fitness statistics and maintains a historical CSV database with calculated averages.

## Architecture
- **Single-file application**: `main.py` handles authentication, data fetching, and CSV updates
- **Data flow**: Login → Fetch last 7 days → Update CSV with deduplication and averages
- **Storage**: CSV file with historical data; first data row contains column averages

## Key Components
- **Authentication**: Uses `garth` library for robust MFA login with token persistence in `~/.garth`
- **Data fetching**: `garminconnect` library for stats (steps, distance, calories, heart rate) and sleep data
- **CSV management**: Updates `garmin_stats_history.csv` by replacing overlapping dates and recalculating averages

## Critical Workflows
- **Run script**: `python main.py` (requires `.env` with GARMIN_EMAIL and GARMIN_PASSWORD)
- **Data update**: Fetches last 7 days to handle missed runs; replaces existing dates
- **Error handling**: Continues on individual date failures; logs errors to console

## Project Conventions
- **CSV structure**: Header row → "MEDIAS" row with averages → chronological data rows
- **Date handling**: ISO format (YYYY-MM-DD); processes from oldest to newest
- **Numeric precision**: Distances rounded to 2 decimals; sleep hours from seconds
- **Token management**: Auto-resumes from `~/.garth`; recreates on failure

## Dependencies & Environment
- **Python packages**: `garth`, `garminconnect`, `python-dotenv`
- **Environment variables**: GARMIN_EMAIL, GARMIN_PASSWORD in `.env`
- **File paths**: CSV in project root; tokens in user home

## Code Patterns
- **Login robustness**: Try resume → fallback to fresh login → configure Garmin client
- **Data extraction**: Stats via `client.get_stats(date)`; sleep via `client.get_sleep_data(date)`
- **CSV update logic**: Read existing (skip averages row) → combine with new → recalculate averages → write all

## Examples
- **Adding new metric**: Follow sleep data pattern - try/except block, extract from API response
- **Modifying CSV columns**: Update `fieldnames` list and row dictionary construction in `get_days_data`
- **Error handling**: Use try/except with print statements for non-critical failures

## Key Files
- `main.py`: Complete application logic
- `garmin_stats_history.csv`: Data storage with averages row
- `.env`: Credentials (not committed)