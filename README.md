# Senzing-Inspired Entity Resolution Engine

This is a high-performance Entity Resolution (ER) engine designed to standardize, match, and resolve chaotic multi-source data (CSV/JSON/Databases) into unified Entity profiles. It strictly adheres to a flattened Senzing Object Specification schema under the hood and applies blazing fast deterministic (and extensible ML) matching algorithms.

## Prerequisites
- **Docker** and **Docker Compose**

## Quick Start (Docker)

1. **Clone the Repository** and cd into the folder.
2. **Build and Run the Engine**:
   ```bash
   docker-compose up --build -d
   ```
3. **Access the Streamlit Dashboard**: Navigate to `http://localhost:8501` to view the UI. The database will automatically initialize locally in the `/data` folder.

## Architecture Highlights
- `er_engine/database/`: SQLAlchemy ORM. Completely DB agnostic (swap the Docker `DATABASE_URL` to postgres and reboot, and it will instantly scale horizontally).
- `er_engine/core/standardizer.py`: NLP text parsers. Generates exact hashes, compound (scoped) hashes, and Phonetic hashes mapping misspelled names to identical hash buckets.
- `er_engine/core/rules_engine.py`: Scoring evaluation layer that compares sets of feature hashes based on configured levels (Level 1: Merge, Level 2/3: Form Relationship Edge).
- `er_engine/api/`: Optimized SQL endpoints mapping thousands of resolved records simultaneously into consolidated snapshots and sub-second searching.
- `ui/app.py`: A comprehensive visual drag-and-drop dashboard to handle bulk ingestion arrays and render connection graphs.

## Documentation
Please view the incredibly critical `FEATURES_AND_RULES_GUIDE.md` for explicit formulas on structuring JSON payloads to correctly leverage the framework's native rules!
