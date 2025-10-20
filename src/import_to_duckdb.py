"""DuckDB importer for NYC Taxi trip data.

This module provides a class to import Parquet files containing NYC Taxi trip
records into a DuckDB database, with duplicate prevention and transaction safety.
"""

from pathlib import Path

import duckdb


class DuckDBImporter:
    """Import NYC Taxi Parquet files into DuckDB with duplicate prevention.

    This class manages the import of NYC Taxi trip data from Parquet files into
    a DuckDB database. It maintains an import log to prevent duplicate imports
    and uses transactions to ensure data integrity.

    Attributes:
        db_path (str): Path to the DuckDB database file.
        conn (duckdb.DuckDBPyConnection): Active DuckDB connection.

    Example:
        >>> importer = DuckDBImporter("data/taxi.duckdb")
        >>> count = importer.import_all_parquet(Path("data/raw"))
        >>> stats = importer.get_statistics()
        >>> importer.close()
    """

    def __init__(self, db_path: str):
        """Initialize the DuckDB importer and create necessary tables.

        Args:
            db_path: Path where the DuckDB database file will be created/opened.
        """
        self.db_path = db_path

        # Create parent folder if necessary (defensive programming)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Connect to DuckDB
        self.conn = duckdb.connect(database=db_path)

        # Initialization
        self._initialize_database()

    def _initialize_database(self):
        """Create required database tables if they don't exist.

        Creates two tables:
        - yellow_taxi_trips: Stores the taxi trip data
        - import_log: Tracks which files have been imported
        """
        # Create table yellow_taxi_trips for taxi data (if it doesn't exist already)
        self.conn.execute("""
                          CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
                            VendorID BIGINT,
                            tpep_pickup_datetime TIMESTAMP,
                            tpep_dropoff_datetime TIMESTAMP,
                            passenger_count DOUBLE,
                            trip_distance DOUBLE,
                            RatecodeID DOUBLE,
                            store_and_fwd_flag VARCHAR,
                            PULocationID BIGINT,
                            DOLocationID BIGINT,
                            payment_type BIGINT,
                            fare_amount DOUBLE,
                            extra DOUBLE,
                            mta_tax DOUBLE,
                            tip_amount DOUBLE,
                            tolls_amount DOUBLE,
                            improvement_surcharge DOUBLE,
                            total_amount DOUBLE,
                            congestion_surcharge DOUBLE,
                            Airport_fee DOUBLE,
                            cdb_congestion_fee DOUBLE -- New field added in 2025
                        );
                        """)
        print("✅ Table yellow_taxi_trips ready.")

        self.conn.execute("""
                          CREATE TABLE IF NOT EXISTS import_log (
                            file_name VARCHAR PRIMARY KEY,
                            import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            rows_imported BIGINT
                        );
                        """)
        print("✅ Table import_log ready.")

    def is_file_imported(self, file_name: str) -> bool:
        """Check if a file has already been imported.

        Args:
            file_name: Name of the Parquet file to check.

        Returns:
            True if the file has been imported before, False otherwise.
        """
        result = self.conn.execute(
            """
                SELECT COUNT(*)
                FROM import_log
                WHERE file_name = ?;
                """,
            (file_name,),
        ).fetchone()
        return result[0] > 0

    def import_parquet(self, file_path: Path) -> bool:
        """Import a single Parquet file into the database.

        Uses a transaction to ensure atomic import: either the data AND log
        entry are both inserted, or neither is (rollback on error).

        Args:
            file_path: Path object pointing to the Parquet file.

        Returns:
            True if import succeeded or file was already imported, False on error.
        """
        filename = file_path.name

        # 1. Check if already imported
        if self.is_file_imported(filename):
            print(f"⏭️ {filename} already imported. Skipping.")
            return True

        try:
            # Begin transaction (all or nothing strategy)
            self.conn.begin()

            # 2. Count rows before import
            count_before = self.conn.execute("""
                                             SELECT COUNT(*)
                                               FROM yellow_taxi_trips;
                                             """).fetchone()[0]

            # 3. Import data (as_posix for cross-platform compatibility)
            self.conn.execute(f"""
                              INSERT INTO yellow_taxi_trips
                              SELECT *
                              FROM read_parquet('{file_path.as_posix()}');
                              """)

            # 4. Count rows after import
            count_after = self.conn.execute("""
                                            SELECT COUNT(*)
                                              FROM yellow_taxi_trips;
                                            """).fetchone()[0]

            rows_imported = count_after - count_before

            # 5. Log the import
            self.conn.execute(
                """
                              INSERT INTO import_log (file_name, rows_imported)
                              VALUES (?, ?);
                              """,
                (filename, rows_imported),
            )

            # Commit transaction
            self.conn.commit()

            print(f"✅ Imported {filename}: {rows_imported} rows.")
            return True

        except Exception as e:
            # Rollback transaction on error
            self.conn.rollback()
            print(f"❌ Failed to import {filename}: {e}")
            return False

    def import_all_parquet(self, data_dir: Path) -> int:
        """Import all Parquet files from a directory.

        Files are processed in chronological order (sorted by filename).

        Args:
            data_dir: Path to directory containing Parquet files.

        Returns:
            Number of files successfully imported (excluding already imported).
        """
        # List and sort all parquet files in the data directory
        parquet_files = sorted(data_dir.glob("*.parquet"))

        # Count imported files
        imported_count = 0

        for file_path in parquet_files:
            if self.import_parquet(file_path):
                imported_count += 1

        return imported_count

    def get_statistics(self) -> dict:
        """Retrieve and display database statistics.

        Returns:
            Dictionary containing:
                - total_trips (int): Total number of trips in database
                - files_imported (int): Number of files imported
                - date_range (tuple): (min_date, max_date) or (None, None)
                - db_size (float): Database size in megabytes
        """
        stats = {
            "total_trips": (
                self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips;").fetchone()[
                    0
                ]
            ),
            "files_imported": (
                self.conn.execute("SELECT COUNT(*) FROM import_log;").fetchone()[0]
            ),
            "date_range": (
                self.conn.execute("""
                                 SELECT
                                     MIN(tpep_pickup_datetime),
                                     MAX(tpep_pickup_datetime)
                                   FROM yellow_taxi_trips;
                                """).fetchone()
            ),
            "db_size": Path(self.db_path).stat().st_size / (1_024 * 1_024),
        }

        print("\n📊 Database Statistics:")
        print(f"   - Total Trips: {stats['total_trips']:,}")
        print(f"   - Files Imported: {stats['files_imported']:,}")
        if stats["date_range"][0] is not None:
            print(
                f"   - Date Range: {stats['date_range'][0]} to {stats['date_range'][1]}"
            )
        else:
            print("   - Date Range: No data")
        print(f"   - Database Size: {stats['db_size']:.2f} MB")

        return stats

    def close(self):
        """Close the DuckDB connection."""
        self.conn.close()


if __name__ == "__main__":
    importer = DuckDBImporter("data/taxi.duckdb")
    try:
        count = importer.import_all_parquet(Path("data/raw"))
        print(f"\n✅ Import complete: {count} files processed.")
        importer.get_statistics()
    finally:
        importer.close()
