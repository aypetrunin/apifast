# apifast — FastAPI HTTP API Gateway

## What This Service Does

HTTP API gateway for the Zena platform. Receives client requests and proxies them to the langgraph orchestration service. Also handles data updates from Google Sheets into PostgreSQL and Qdrant.

## Project Structure

```
apifast/
├── main.py                  # FastAPI app entrypoint
├── src/
│   ├── settings.py          # Pydantic Settings config
│   ├── deps.py              # Dependency injection
│   ├── common.py            # Shared utilities
│   ├── schemas.py           # Pydantic request/response schemas
│   ├── routes/              # API route handlers
│   │   ├── agent.py         # Main agent endpoint (proxies to langgraph)
│   │   ├── health.py        # Health check
│   │   ├── reminders.py     # Reminder endpoints
│   │   ├── update_faq.py
│   │   ├── update_services.py
│   │   ├── update_products.py
│   │   └── update_promo.py
│   ├── update/              # Data sync: Google Sheets -> Postgres/Qdrant
│   │   ├── google_sheet_reader.py
│   │   ├── postgres_*.py
│   │   └── qdrant_*.py
│   └── requests/
│       └── httpservice.py   # HTTP client for langgraph calls
```

## Common Commands

```bash
# Run locally
uvicorn main:app --reload --host 0.0.0.0 --port 3025

# Install deps
uv sync

# Lint & format
uv run ruff check src/
uv run ruff format src/
uv run mypy src/
```

## Code Style

- ruff (line-length=88), mypy strict with pydantic plugin
- Google-style docstrings
- No prints (use structlog)
