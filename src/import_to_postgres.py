"""PostgreSQL importer for NYC taxi trip data using SQLModel and pandas."""

import io
from pathlib import Path
import re

import pyarrow.parquet as pq
from sqlmodel import Session, select

from database import engine, init_db
from models import ImportLog, YellowTaxiTrip  # noqa: F401


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
            # 2. Read Parquet file with chunking
            parquet_file = pq.ParquetFile(file_path)
            total_rows = 0
            chunk_size = 500_000

            print(f"📦 Processing {filename} in chunks of {chunk_size:,} rows...")

            # Get raw connection once for all chunks
            connection = engine.raw_connection()

            try:
                for batch in parquet_file.iter_batches(batch_size=chunk_size):
                    df = batch.to_pandas()

                    # Rename columns to snake_case
                    df = df.rename(columns=to_snake_case)

                    # Create CSV buffer in memory
                    buffer = io.StringIO()
                    df.to_csv(buffer, index=False, header=False)
                    buffer.seek(0)

                    # Import chunk using COPY
                    cursor = connection.cursor()
                    cursor.copy_expert(
                        f"COPY yellow_taxi_trips ({', '.join(df.columns)}) FROM STDIN WITH CSV",
                        buffer,
                    )

                    total_rows += len(df)
                    print(f"  ✓ Imported {total_rows:,} rows...")

                connection.commit()
                print(f"✅ Completed import: {total_rows:,} rows total")

            finally:
                connection.close()

            # 3. Log the import in database
            with Session(engine) as session:
                log_entry = ImportLog(file_name=filename, rows_imported=total_rows)
                session.add(log_entry)
                session.commit()

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
