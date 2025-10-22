"""Business logic for taxi trip operations."""

from sqlmodel import Session, func, select

from models import YellowTaxiTrip
from schemas import TaxiTripCreate, TaxiTripUpdate, Statistics


class TaxiTripService:
    """Service class for taxi trip operations."""

    @staticmethod
    def get_trip(db: Session, trip_id: int) -> YellowTaxiTrip | None:
        """Get a single trip by ID."""
        return db.get(YellowTaxiTrip, trip_id)

    @staticmethod
    def get_trips(
        db: Session, skip: int = 0, limit: int = 100
    ) -> tuple[list[YellowTaxiTrip], int]:
        """Get paginated list of trips.

        Returns:
            Tuple of (trips, total_count)
        """
        statement = select(YellowTaxiTrip).offset(skip).limit(limit)
        trips = list(db.exec(statement).all())

        # Get total count
        count_statement = select(func.count()).select_from(YellowTaxiTrip)
        total = db.exec(count_statement).one()

        return trips, total

    @staticmethod
    def create_trip(db: Session, trip: TaxiTripCreate) -> YellowTaxiTrip:
        """Create a new taxi trip."""
        db_trip = YellowTaxiTrip(**trip.model_dump())
        db.add(db_trip)
        db.commit()
        db.refresh(db_trip)
        return db_trip

    @staticmethod
    def update_trip(
        db: Session, trip_id: int, trip: TaxiTripUpdate
    ) -> YellowTaxiTrip | None:
        """Update an existing taxi trip."""
        db_trip = db.get(YellowTaxiTrip, trip_id)
        if not db_trip:
            return None

        # Update only provided fields
        trip_data = trip.model_dump(exclude_unset=True)
        for key, value in trip_data.items():
            setattr(db_trip, key, value)

        db.add(db_trip)
        db.commit()
        db.refresh(db_trip)
        return db_trip

    @staticmethod
    def delete_trip(db: Session, trip_id: int) -> bool:
        """Delete a taxi trip by ID."""
        db_trip = db.get(YellowTaxiTrip, trip_id)
        if not db_trip:
            return False

        db.delete(db_trip)
        db.commit()
        return True

    @staticmethod
    def get_statistics(db: Session) -> Statistics:
        """Get aggregated statistics."""
        # Total trips
        total_trips = db.exec(select(func.count()).select_from(YellowTaxiTrip)).one()

        # Average trip distance
        avg_distance = db.exec(select(func.avg(YellowTaxiTrip.trip_distance))).one()

        # Average fare amount
        avg_fare = db.exec(select(func.avg(YellowTaxiTrip.fare_amount))).one()

        # Total revenue
        total_revenue = db.exec(select(func.sum(YellowTaxiTrip.total_amount))).one()

        return Statistics(
            total_trips=total_trips,
            avg_distance=round(avg_distance, 2) if avg_distance else 0,
            avg_fare=round(avg_fare, 2) if avg_fare else 0,
            total_revenue=round(total_revenue, 2) if total_revenue else 0,
        )
