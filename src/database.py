"""Database configuration and connection management for PostgreSQL."""

import os
from typing import Iterator

from sqlmodel import SQLModel, create_engine, Session


# Database configuration from environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nyc_taxi")
APP_ENV = os.getenv("APP_ENV", "development")

# Build DATABASE_URL
DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Create engine
engine = create_engine(DATABASE_URL, echo=False)


# ===== UTILITY FUNCTIONS =====
def init_db():
    """Create all database tables.

    This function should be called once to initialize the database schema.
    It creates all tables defined in SQLModel models.
    """
    SQLModel.metadata.create_all(engine)
    print("🌱 Database tables created successfully.")


def get_db() -> Iterator[Session]:
    """Dependency function to get database session.

    Yields a database session and ensures it is closed after use.
    Used as a FastAPI dependency for endpoints.

    Yields:
        Session: A SQLModel database Session instance.
    """
    with Session(engine) as session:
        yield session
