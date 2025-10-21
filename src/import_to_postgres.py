"""PostgreSQL importer for NYC taxi trip data using SQLModel and pandas."""

from datetime import datetime
import io
from pathlib import Path
import re

import pandas as pd
from sqlmodel import Field, Session, SQLModel, select

from database import engine, init_db


# ===== SQLMODEL MODELS =====
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

    file_name: str = Field(primary_key=True)
    import_date: datetime = Field(default_factory=datetime.now)
    rows_imported: int


# ===== UTILITY FUNCTION =====
def to_snake_case(name: str) -> str:
    """Convert PascalCase/camelCase to snake_case, handling acronyms.

    Examples:
        VendorID -> vendor_id
        PULocationID -> pu_location_id
        RatecodeID -> ratecode_id
    """
    # Insert underscore between lowercase/digit and uppercase
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
    # Insert underscore between multiple uppercase and uppercase+lowercase
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return name.lower()


# ===== IMPORTER CLASS =====
class PostgresImporter:
    """Importer NYC Taxi Parquet files into PostgreSQL using pandas and SQLModel."""

    def __init__(self):
        """Initialize the PostgreSQL importer and create tables."""
        # Initialize database tables
        init_db()

    def is_file_imported(self, filename: str) -> bool:
        """Check if a file has already been imported.

        Args:
            filename: Name of the parquet file to check.

        Returns:
            True if the file has been imported before, False otherwise.
        """
        with Session(engine) as session:
            statement = select(ImportLog).where(ImportLog.file_name == filename)
            result = session.exec(statement).first()
            return result is not None

    def import_parquet(self, file_path: Path) -> bool:
        """Import a single Parquet file into PostgreSQL.

        Args:
            file_path: Path to the Parquet file.

        Returns:
            True if import succeeded, False on error.
        """
        filename = file_path.name

        # 1. Check if already imported
        if self.is_file_imported(filename):
            print(f"⏭️ {filename} already imported. Skipping.")
            return True

        try:
            # 2. Read Parquet file into DataFrame
            df = pd.read_parquet(file_path)

            # 3. Rename columns to snake_case
            df = df.rename(columns=to_snake_case)

            # 4. Count rows to import
            rows_imported = len(df)

            # 5. Import data using PostgreSQL COPY (fastest method)
            print(f"💾 Starting bulk import for {rows_imported:,} rows using COPY...")

            # Create an in-memory buffer
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            # Use COPY FROM for ultra-fast insertion
            connection = engine.raw_connection()

            try:
                cursor = connection.cursor()
                cursor.copy_expert(
                    f"COPY yellow_taxi_trips ({','.join(df.columns)}) FROM STDIN WITH CSV",
                    buffer,
                )
                connection.commit()
            finally:
                connection.close()
            print("✓ Bulk import completed")

            # 6. Log the import in database
            with Session(engine) as session:
                log_entry = ImportLog(file_name=filename, rows_imported=rows_imported)
                session.add(log_entry)
                session.commit()

            print(f"✅ Imported {filename}: {rows_imported:,} rows.")
            return True

        except Exception as e:
            print(f"❌ Error importing {filename}: {e}")
            return False

    def import_all_parquet(self, data_dir: Path) -> int:
        """Import all Parquet files from a directory into PostgreSQL.

        Args:
            data_dir: Directory containing Parquet files.

        Returns:
            Number of files successfully imported.
        """
        # List and sort all parquet files in the data directory
        parquet_files = sorted(data_dir.glob("*.parquet"))

        # Count imported files
        imported_count = 0

        for file_path in parquet_files:
            if self.import_parquet(file_path):
                imported_count += 1

        return imported_count


if __name__ == "__main__":
    importer = PostgresImporter()
    data_directory = Path("data/raw")

    count = importer.import_all_parquet(data_directory)
    print(f"\n📊 Summary: {count} file(s) processed.")
