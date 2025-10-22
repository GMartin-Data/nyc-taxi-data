"""FastAPI application for NYC Taxi Trip data."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import SQLModel

from database import engine
from routes import router


# Create database tables
SQLModel.metadata.create_all(engine)


# Create FastAPI app
app = FastAPI(
    title="NYC Taxi Data Pipeline API",
    description="API for querying and managing NYC Yellow Taxi trip data",
    version="1.0.0",
)


# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include router with prefix
app.include_router(router, prefix="/api/v1")


@app.get("/", tags=["Root"])
def root():
    """Root endpoint with API information."""
    return {
        "name": "NYC Taxi Data Pipeline API",
        "version": "1.0.0",
        "description": "API for querying NYC Yellow Taxi trip data",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
