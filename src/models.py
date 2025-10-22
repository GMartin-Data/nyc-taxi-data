"""SQLModel models for database tables."""

from datetime import datetime

from sqlmodel import SQLModel, Field


class YellowTaxiTrip(SQLModel, table=True):
    """SQLModel for yellow_taxi_trips table."""

    __tablename__: str = "yellow_taxi_trips"

    id: int | None = Field(default=None, primary_key=True)
    vendor_id: int | None = Field(default=None)
    tpep_pickup_datetime: datetime | None = Field(default=None)
    tpep_dropoff_datetime: datetime | None = Field(default=None)
    passenger_count: float | None = Field(default=None)
    trip_distance: float | None = Field(default=None)
    ratecode_id: float | None = Field(default=None)
    store_and_fwd_flag: str | None = Field(default=None)
    pu_location_id: int | None = Field(default=None)
    do_location_id: int | None = Field(default=None)
    payment_type: int | None = Field(default=None)
    fare_amount: float | None = Field(default=None)
    extra: float | None = Field(default=None)
    mta_tax: float | None = Field(default=None)
    tip_amount: float | None = Field(default=None)
    tolls_amount: float | None = Field(default=None)
    improvement_surcharge: float | None = Field(default=None)
    total_amount: float | None = Field(default=None)
    congestion_surcharge: float | None = Field(default=None)
    airport_fee: float | None = Field(default=None)
    cbd_congestion_fee: float | None = Field(default=None)


class ImportLog(SQLModel, table=True):
    """SQLModel for import_log table."""

    __tablename__: str = "import_log"

    id: int | None = Field(default=None, primary_key=True)
    file_name: str = Field(max_length=255)
    import_date: datetime = Field(default_factory=datetime.utcnow)
    rows_imported: int
