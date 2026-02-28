import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from er_engine.database.schema import Base

# Determine Database connection string (defaulting to SQLite mapped to our data directory)
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'er_engine.db'))
DEFAULT_DATABASE_URL = f"sqlite:///{DB_PATH}"

DATABASE_URL = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    import er_engine.database.schema  # purely to load the models before creating
    Base.metadata.create_all(bind=engine)

def purge_db():
    """Drops all tables and recreates them, effectively purging all data."""
    import er_engine.database.schema
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
