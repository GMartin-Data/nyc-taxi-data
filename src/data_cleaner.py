"""Data cleaning and MongoDB integration for NYC taxi data."""

import os
from typing import Iterator

import pandas as pd
from pymongo import MongoClient

from database import engine


class DataCleaner:
    """Clean taxi trip data and save it to MongoDB."""

    def __init__(self, sample_size: int | None = None, chunk_size: int = 500_000):
        """Initialize connections and processing parameters.

        Args:
            sample_size: Limit number of rows to process (None = all data)
            chunk_size: Size of chunks for processing large datasets
        """
        self.sample_size = sample_size
        self.chunk_size = chunk_size

        self.postgres_engine = engine
        self.mongo_client = self._get_mongo_client()
        self.db = self.mongo_client[os.getenv("MONGO_DB", "nyc_taxi_clean")]
        self.collection = self.db["cleaned_trips"]

        if sample_size:
            print(f"🔌 Connections established (sample mode: {sample_size:,} rows)")
        else:
            print(
                f"🔌 Connections established (full mode with chunking: {chunk_size:,} rows/chunk)"
            )

    def _get_mongo_client(self) -> MongoClient:
        """Create MongoDB client from environment variables."""
        user = os.getenv("MONGO_USER", "admin")
        password = os.getenv("MONGO_PASSWORD", "admin")
        host = os.getenv("MONGO_HOST", "mongodb")
        port = os.getenv("MONGO_PORT", "27017")

        url = f"mongodb://{user}:{password}@{host}:{port}/"
        return MongoClient(url)

    def load_data_chunks(self) -> Iterator[pd.DataFrame]:
        """Load data from PostgreSQL in chunks.

        Yields:
            DataFrame of chunk_size rows
        """
        print("\n📥 Loading data from PostgreSQL in chunks...")

        if self.sample_size:
            # Sample mode: load limited data in one chunk
            query = f"""
            SELECT *
              FROM yellow_taxi_trips
             LIMIT {self.sample_size}
            """
            df = pd.read_sql(query, self.postgres_engine)
            yield df
        else:
            # Full mode: load by chunks
            query = """
            SELECT *
              FROM yellow_taxi_trips
            """
            chunks = pd.read_sql(query, self.postgres_engine, chunksize=self.chunk_size)

            chunk_num = 0
            for chunk in chunks:
                chunk_num += 1
                print(f"  ✓ Processing chunk {chunk_num}: {len(chunk):,} rows")
                yield chunk

    # 🚧 CRITERIA CAN BE ADAPTED
    def analyze_chunk(self, df: pd.DataFrame) -> dict:
        """Analyze data for anomalies and outliers.

        Args:
            df: DataFrame chunk to analyze

        Returns:
            Dictionary with anomaly statistics
        """
        print("\n🔍 Analyzing data for anomalies...")

        analysis = {}

        # 1. Negative values
        negative_cols = [
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount",
        ]

        for col in negative_cols:
            if col in df.columns:  # Defensive check
                neg_count = (df[col] < 0).sum()
                if neg_count > 0:
                    analysis[f"{col}_negative"] = neg_count
                    print(f"  ⚠️ {col}: {neg_count:,} negative values.")

        # 2. Null datetime values
        null_pickup = df.tpep_pickup_datetime.isna().sum()
        null_dropoff = df.tpep_dropoff_datetime.isna().sum()
        if null_pickup > 0:
            analysis["tpep_pickup_datetime_null"] = null_pickup
            print(f"  ⚠️ tpep_pickup_datetime: {null_pickup:,} null values.")
        if null_dropoff > 0:
            analysis["tpep_dropoff_datetime_null"] = null_dropoff
            print(f"  ⚠️ tpep_dropoff_datetime: {null_dropoff:,} null values.")

        # 3. Outliers
        invalid_passengers = ((df.passenger_count < 1) | (df.passenger_count > 8)).sum()
        if invalid_passengers > 0:
            analysis["passenger_count_outliers"] = invalid_passengers
            print(f"  ⚠️ passenger_count: {invalid_passengers:,} outliers (not in 1-8).")

        long_trips = (df.trip_distance > 100).sum()
        if long_trips > 0:
            analysis["long_trip_distance"] = long_trips
            print(f"  ⚠️ trip_distance: {long_trips:,} trips > 100 miles.")

        high_fares = (df.fare_amount > 500).sum()
        if high_fares > 0:
            analysis["high_fare_amount"] = high_fares
            print(f"  ⚠️ fare_amount: {high_fares:,} fares > $500.")

        if not analysis:
            print("  ✅ No anomalies detected.")

        return analysis

    def clean_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean a single chunk by removing anomalies and outliers.

        Args:
            df: DataFrame chunk to clean

        Returns:
            Cleaned DataFrame chunk
        """
        print("\n🧹 Cleaning data...")

        initial_count = len(df)

        # 1. Remove negative values
        negative_cols = [
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount",
        ]
        for col in negative_cols:
            if col in df.columns:
                before = len(df)
                df = df.query(f"{col} >= 0")
                removed = before - len(df)
                if removed > 0:
                    print(f"  ✓ Removed {removed:,} rows with {col} < 0")

        # 2. Remove null datetime values
        before = len(df)
        df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
        removed = before - len(df)
        if removed > 0:
            print(f"  ✓ Removed {removed:,} rows with null datetime")

        # 3. Remove invalid passenger count
        before = len(df)
        df = df.query("1 <= passenger_count <= 8")
        removed = before - len(df)
        if removed > 0:
            print(f"  ✓ Removed {removed:,} rows with invalid passenger_count")

        # 4. Remove long trips
        before = len(df)
        df = df.query("trip_distance <= 100")
        removed = before - len(df)
        if removed > 0:
            print(f"  ✓ Removed {removed:,} rows with trip_distance > 100 miles")

        # 5. Remove high fares
        before = len(df)
        df = df.query("fare_amount <= 500")
        removed = before - len(df)
        if removed > 0:
            print(f"  ✓ Removed {removed:,} rows with fare_amount > $500")

        # Summary
        total_removed = initial_count - len(df)
        percentage = (total_removed / initial_count) * 100

        print(f"\n  📈 Total removed: {total_removed:,} rows ({percentage:.2f}%)")
        print(f"  ✅ Remaining rows: {len(df):,}")

        return df

    def process_all(self) -> dict:
        """Process all data by chunks: analyze, clean, and save to MongoDB.

        Returns:
            Dictionary with aggregated statistics
        """
        print("\n🚀 Starting data processing pipeline...")

        # Aggregated statistics
        total_loaded = 0
        total_cleaned = 0
        aggregated_analysis = {}
        first_chunk = True

        # Process each chunk
        for chunk_df in self.load_data_chunks():
            # Analyze chunk
            chunk_analysis = self.analyze_chunk(chunk_df)

            # Aggregate analysis results
            for key, value in chunk_analysis.items():
                aggregated_analysis[key] = aggregated_analysis.get(key, 0) + value

            # Clean chunk
            cleaned_chunk = self.clean_chunk(chunk_df)

            # Save to MongoDB (clear collection only on first chunk)
            if first_chunk:
                existing = self.collection.count_documents({})
                if existing > 0:
                    print(
                        f"\n💾 Clearing {existing:,} existing documents from MongoDB..."
                    )
                    self.collection.delete_many({})
                first_chunk = False

            # Insert cleaned chunk
            self.save_to_mongodb(cleaned_chunk, show_delete_warning=False)

            # Update totals
            total_loaded += len(chunk_df)
            total_cleaned += len(cleaned_chunk)

        # Final summary
        total_removed = total_loaded - total_cleaned
        percentage = (total_removed / total_loaded) * 100 if total_loaded > 0 else 0

        print(f"\n{'=' * 60}")
        print("📊 PIPELINE SUMMARY")
        print(f"{'=' * 60}")
        print(f"  Total loaded:   {total_loaded:,} rows")
        print(f"  Total cleaned:  {total_cleaned:,} rows")
        print(f"  Total removed:  {total_removed:,} rows ({percentage:.2f}%)")
        print(f"  Saved to MongoDB: {total_cleaned:,} documents")

        return {
            "total_loaded": total_loaded,
            "total_cleaned": total_cleaned,
            "total_removed": total_removed,
            "anomalies": aggregated_analysis,
        }

    def save_to_mongodb(
        self, df: pd.DataFrame, show_delete_warning: bool = True
    ) -> int:
        """Save cleaned data to MongoDB.

        Args:
            df: Cleaned DataFrame to save
            show_delete_warning: Whether to check and delete existing data

        Returns:
            Number of documents inserted
        """
        print("\n💾 Saving to MongoDB...")

        # Check and delete existing data (only if show_delete_warning is True)
        if show_delete_warning:
            existing_count = self.collection.count_documents({})
            if existing_count > 0:
                print(f"  ⚠️  Found {existing_count:,} existing documents, deleting...")
                self.collection.delete_many({})

        # Convert DataFrame to a list of dictionaries
        records = df.to_dict(orient="records")

        # Process records: remove PostgreSQL ID and convert timestamps
        for record in records:
            if "id" in record:
                del record["id"]

            # Convert Pandas Timestamp to Python datetime
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.to_pydatetime()

        # Insert into MongoDB
        result = self.collection.insert_many(records)
        inserted_count = len(result.inserted_ids)

        print(f"  ✅ Inserted {inserted_count:,} documents into MongoDB.")
        return inserted_count

    def close(self) -> None:
        """Close MongoDB connection."""
        self.mongo_client.close()
        print("\n🔒 Connections closed")


if __name__ == "__main__":
    cleaner = DataCleaner(chunk_size=100_000)
    try:
        stats = cleaner.process_all()
    finally:
        cleaner.close()
