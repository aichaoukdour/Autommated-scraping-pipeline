# ADIL Scraper - Real-Time Data Pipeline

A production-ready web scraping pipeline with Clean Architecture, real-time monitoring, and data transformation capabilities.

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12+
- Playwright browsers installed

### Setup

1. **Start all services (recommended):**
```bash
docker-compose up -d
```

This starts:
- ✅ PostgreSQL (database)
- ✅ Redis (cache + Celery broker)
- ✅ Elasticsearch (search)
- ✅ Prometheus (metrics)
- ✅ Grafana (dashboards)
- ✅ FastAPI (REST API at http://localhost:8000)
- ✅ Celery Worker (async tasks)
- ✅ Celery Beat (scheduler)
- ✅ Flower (Celery monitoring at http://localhost:5555)

2. **Access services:**
- **API**: http://localhost:8000/docs
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Flower**: http://localhost:5555

3. **Index existing data in Elasticsearch:**
```bash
python index_elasticsearch.py
```

4. **Alternative - Local development:**
```bash
# Start only database services
docker-compose up -d postgres redis elasticsearch

# Install dependencies
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
playwright install chromium

# Run pipeline
python run_pipeline.py --code 0804100000 --interval 5
```

## 📊 Data Pipeline

```
Scraping → Raw Data (scraped_data) → ETL → Cleaned Data (cleaned_data)
         ↓
    Elasticsearch (search) + Prometheus (metrics)
```

### Access Data via API (Recommended)
```bash
# List all codes
curl http://localhost:8000/api/tariff-codes

# Get specific code
curl http://localhost:8000/api/tariff-codes/0804100000

# Search
curl "http://localhost:8000/api/search?q=coconut"

# Trigger scrape (async)
curl -X POST http://localhost:8000/api/scrape/0804100000

# View API docs
# Open http://localhost:8000/docs in browser
```

### Command-Line Tools (Alternative)
```bash
# View raw data
python view_data.py list
python view_data.py view <tariff_code>

# Transform data (ETL)
python run_etl.py <tariff_code>  # Transform one
python run_etl.py                # Transform all

# View cleaned data
python view_cleaned_data.py list
python view_cleaned_data.py view <tariff_code>
```

## 🏗️ Architecture

Clean Architecture with:
- **Domain Layer**: Entities, Value Objects, Repository Interfaces
- **Application Layer**: Use Cases, DTOs, Transformers
- **Infrastructure Layer**: Playwright, PostgreSQL, Redis
- **Presentation Layer**: CLI, Formatters

See `ARCHITECTURE.md` for details.

## 📁 Project Structure

```
src/
├── domain/           # Business entities and interfaces
├── application/      # Use cases and business logic
├── infrastructure/   # External integrations
└── presentation/      # CLI and output formatting

docker/               # Docker configuration
  └── postgres/       # Database init scripts
```

## 🔧 Configuration

- **Database**: PostgreSQL (port 5433)
- **Cache**: Redis (port 6379)
- **Monitoring**: Real-time scraping with change detection
- **ETL**: Automatic data cleaning and normalization

## 📚 Documentation

- `ARCHITECTURE.md` - System architecture details
- `SQL_QUERIES.md` - SQL queries for database access
- `INTEGRATION_GUIDE.md` - How to integrate other stacks
- `RECOMMENDED_STACKS.md` - Recommended stacks (prioritized suggestions)
- `DATA_DRIVEN_RECOMMENDATIONS.md` - Stack recommendations based on your actual data
- `STACK_IMPLEMENTATION.md` - **How to use the implemented stacks** (FastAPI, Elasticsearch, Celery, etc.)

## 🛠️ Utility Scripts

- `run_etl.py` - Run ETL transformation on scraped data
- `scrape_now.py <tariff_code>` - Manually scrape a single tariff code
- `view_data.py` - View raw scraped data
- `view_cleaned_data.py` - View cleaned/transformed data
- `view_json.py` - View data in JSON format
- `check_hash.py` - Check hash status for deduplication

## 🐛 Troubleshooting

**Headless mode issues**: Currently scraping works with `headless=False`. See logs for details.

**Database connection**: Ensure Docker containers are running and ports are correct.

## 📝 License

[Your License Here]

