# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MSStats is a Python tool for extracting Google Cloud MemoryStore (Redis) database metrics using the Google Cloud Monitoring API. It processes Redis databases (single instance and replicated) across multiple GCP service accounts and generates Excel reports with usage statistics.

## Architecture

- **Single Python script**: `msstats.py` - Main application that processes Redis metrics
- **Batch processing**: Shell scripts for bulk operations across multiple GCP projects
- **Service account management**: Scripts to grant/revoke monitoring permissions
- **Output format**: Excel files with detailed Redis command statistics and throughput data

## Key Components

- **Metric Processing**: Categorizes Redis commands by type (Get, Set, Hash, List, etc.) from monitoring data
- **Multi-node handling**: Aggregates metrics across Redis cluster nodes taking maximum values
- **Time series data**: Configurable duration (default 7 days) and step intervals (default 60s)
- **GCP Integration**: Uses Google Cloud Monitoring API with service account authentication

## Development Commands

### Setup Environment
```bash
# Create and activate virtual environment
python -m venv .env && source .env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Place service account JSON files in root directory
cp path/to/service_account.json .
```

### Running the Tool
```bash
# Basic usage (7 days, 60s steps)
python msstats.py

# Custom duration and step size
python msstats.py --duration 1800 --step 300

# Specific project
python msstats.py -p project-id
```

### Batch Operations
```bash
# Grant monitoring permissions to service account
./grant_sa_monitoring_viewer.sh service-account@project.iam.gserviceaccount.com

# Run across all accessible projects
./batch_run_msstats.sh

# Remove monitoring permissions
./remove_sa_monitoring_viewer.sh service-account@project.iam.gserviceaccount.com

# Clean up environment
deactivate
```

## Command Line Options

- `--duration SECONDS`: Time period to analyze (default: 604800 = 7 days)
- `--step SECONDS`: Metric sampling interval (default: 60 seconds)
- `-p PROJECT_ID`: Target specific GCP project

### Testing and Code Quality
```bash
# Run all tests (unit and integration)
pytest test_msstats.py

# Format code with Black
black *.py

# Check formatting without making changes
black --check *.py
```

## Dependencies

- `openpyxl>=3.0.4`: Excel file generation
- `google-cloud-monitoring==2.18.0`: GCP Monitoring API client
- `black>=25.1.0`: Code formatter
- `pytest>=8.4.0`: Testing framework
- `pytest-mock>=3.14.0`: Mock utilities for testing
- Python 3.9+ required

## Security Notes

- Service account JSON files must be placed in repository root
- Tool uses read-only monitoring API access
- Never connects directly to Redis instances
- No impact on database performance or data