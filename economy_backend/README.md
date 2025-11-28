# Economy Backend

This project provides a FastAPI backend for economics and markets analytics, including configuration through environment variables and Postgres connectivity via SQLAlchemy and Alembic.

## Prerequisites
- Python 3.11+
- Access to a Postgres database

## Setup
1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables (example):
   ```bash
   export APP_NAME="Economy Analytics API"
   export ENV=dev
   export DEBUG=true
   export POSTGRES_URL="postgresql+psycopg2://user:password@localhost:5432/economy"
   export REDIS_URL="redis://localhost:6379/0"
   export FRED_API_KEY="your_fred_key"
   export EIA_API_KEY="your_eia_key"
   export COMTRADE_API_KEY="your_comtrade_key"
   export AISSTREAM_API_KEY="your_aisstream_key"
   ```

## Database migrations
Alembic is configured to use the application database engine. With the environment variables set, run migrations with:
```bash
alembic upgrade head
```

If you want to create a new migration, generate it using:
```bash
alembic revision --autogenerate -m "describe changes"
```

## Running the server
Start the FastAPI application with uvicorn:
```bash
uvicorn app.main:app --reload
```

The root endpoint (`/`) returns application metadata, and `/health` validates database connectivity.
