"""API Routes for taxi trip operations."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlmodel import Session, select, func

from database import get_db
from models import ImportLog
from schemas import (
    TaxiTrip,
    TaxiTripCreate,
    TaxiTripList,
    TaxiTripUpdate,
    Statistics,
    PipelineResponse,
)
from services import TaxiTripService


router = APIRouter()


@router.get(
    "/trips",
    response_model=TaxiTripList,
    status_code=status.HTTP_200_OK,
    tags=["Trips"],
)
def get_trips(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1_000, description="Number of records to return"),
    db: Session = Depends(get_db),
):
    """Get paginated list of taxi trips."""
    trips, total = TaxiTripService.get_trips(db, skip=skip, limit=limit)
    return TaxiTripList(
        total=total,
        skip=skip,
        limit=limit,
        trips=[TaxiTrip.model_validate(trip) for trip in trips],
    )


@router.get(
    "/trips/{trip_id}",
    response_model=TaxiTrip,
    status_code=status.HTTP_200_OK,
    tags=["Trips"],
)
def get_trip(trip_id: int, db: Session = Depends(get_db)):
    """Get a single trip by ID."""
    trip = TaxiTripService.get_trip(db, trip_id)
    if not trip:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trip not found"
        )
    return TaxiTrip.model_validate(trip)


@router.post(
    "/trips",
    response_model=TaxiTrip,
    status_code=status.HTTP_201_CREATED,
    tags=["Trips"],
)
def create_trip(trip: TaxiTripCreate, db: Session = Depends(get_db)):
    """Create a new taxi trip."""
    db_trip = TaxiTripService.create_trip(db, trip)
    return TaxiTrip.model_validate(db_trip)


@router.put(
    "/trips/{trip_id}",
    response_model=TaxiTrip,
    status_code=status.HTTP_200_OK,
    tags=["Trips"],
)
def update_trip(trip_id: int, trip: TaxiTripUpdate, db: Session = Depends(get_db)):
    """Update an existing taxi trip."""
    db_trip = TaxiTripService.update_trip(db, trip_id, trip)
    if not db_trip:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trip not found"
        )
    return TaxiTrip.model_validate(db_trip)


@router.delete(
    "/trips/{trip_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Trips"],
)
def delete_trip(trip_id: int, db: Session = Depends(get_db)):
    """Delete a taxi trip by ID."""
    deleted = TaxiTripService.delete_trip(db, trip_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trip not found"
        )
    # 204 No Content has no response body
    return None


@router.get(
    "/statistics",
    response_model=Statistics,
    status_code=status.HTTP_200_OK,
    tags=["Statistics"],
)
def get_statistics(db: Session = Depends(get_db)):
    """Get statistics about taxi trips."""
    return TaxiTripService.get_statistics(db)


@router.post(
    "/pipeline/run",
    response_model=PipelineResponse,
    status_code=status.HTTP_200_OK,
    tags=["Pipeline"],
)
def run_pipeline(db: Session = Depends(get_db)):
    """Execute the data download and import pipeline."""
    try:
        from pathlib import Path
        from download_data import NYCTaxiDataDownloader
        from import_to_postgres import PostgresImporter

        # Download data
        downloader = NYCTaxiDataDownloader(2_025)
        downloader.download_all_available()

        # Import to PostgreSQL
        importer = PostgresImporter()
        data_dir = Path("data/raw")
        files_imported = importer.import_all_parquet(data_dir)

        # Calculate total rows imported from import_log
        total_rows = db.exec(select(func.sum(ImportLog.rows_imported))).one() or 0

        return PipelineResponse(
            status="success",
            message="Pipeline executed successfully",
            files_processed=files_imported,
            rows_imported=total_rows,
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline failed: {str(e)}",
        )
