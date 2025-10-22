"""Pydantic schemas for request/response validation."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class TaxiTripBase(BaseModel):
    """Base schema for taxi trip."""

    vendor_id: int | None = None
    tpep_pickup_datetime: datetime | None = None
    tpep_dropoff_datetime: datetime | None = None
    passenger_count: float | None = None
    trip_distance: float | None = None
    ratecode_id: float | None = None
    store_and_fwd_flag: str | None = None
    pu_location_id: int | None = None
    do_location_id: int | None = None
    payment_type: int | None = None
    fare_amount: float | None = None
    extra: float | None = None
    mta_tax: float | None = None
    tip_amount: float | None = None
    tolls_amount: float | None = None
    improvement_surcharge: float | None = None
    total_amount: float | None = None
    congestion_surcharge: float | None = None
    airport_fee: float | None = None
    cbd_congestion_fee: float | None = None


class TaxiTripCreate(TaxiTripBase):
    """Schema for creating a taxi trip."""

    pass


class TaxiTripUpdate(TaxiTripBase):
    """Schema for updating a taxi trip."""

    pass


class TaxiTrip(TaxiTripBase):
    """Schema for taxi trip response (with id)."""

    id: int
    model_config = ConfigDict(from_attributes=True)


class TaxiTripList(BaseModel):
    """Schema for paginated list of trips."""

    total: int
    skip: int
    limit: int
    trips: list[TaxiTrip]


class Statistics(BaseModel):
    """Schema for statistics response."""

    total_trips: int
    avg_distance: float
    avg_fare: float
    total_revenue: float


class PipelineResponse(BaseModel):
    """Schema for pipeline execution response."""

    status: str
    message: str
    files_processed: int | None = None
    rows_imported: int | None = None
