"""DLT pipeline for NYC Taxi data."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Iterator

import dlt
import pandas as pd
import requests


# ===== UTILITY FUNCTIONS =====
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


@dlt.resource(name="yellow_taxi_trips", write_disposition="append")
def load_taxi_data(pipeline_instance: NYCTaxiDLTPipeline) -> Iterator[dict[str, Any]]:
    """Load and yield taxi trip records."""
    # 1. For each month
    for month in range(1, 2):  # test with 1 month at first
        # 2. Download if needed
        file_path = pipeline_instance._download_if_needed(month)

        # 3. Read the Parquet file
        df = pd.read_parquet(file_path)

        # 4. Clean the data
        df = pipeline_instance._clean_data(df)

        # 5. Normalize columns (snake_case)
        df = df.rename(columns=to_snake_case)

        # 6. Yield each row as dict
        for record in df.to_dict("records"):
            yield record


class NYCTaxiDLTPipeline:
    """DLT pipeline to download, clean, and load NYC taxi data."""

    def __init__(self):
        """Initialize pipeline with configuration."""
        self.base_url = (
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"
        )
        self.year = dlt.config["sources.nyc_taxi.year"]
        self.data_dir = Path("data/raw")

        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _download_if_needed(self, month: int) -> Path:
        """Download parquet file for a given month if not already present."""
        filename = f"yellow_tripdata_{self.year}-{month:02d}.parquet"
        file_path = self.data_dir / filename

        # Check if file already exists
        if file_path.exists():
            print(
                f"⏭️ File for {self.year}-{month:02d} already exists. Skipping download."
            )
            return file_path

        # Download file
        url = f"{self.base_url}_{self.year}-{month:02d}.parquet"
        print(f"📥 Downloading {filename}...")

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8_192):
                    file.write(chunk)

            print(f"✅ Downloaded {filename} successfully.")

        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")
            if file_path.exists():
                file_path.unlink()
            raise

        return file_path

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean data by removing anomalies."""
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

    def run_pipeline(self) -> Any:
        """Execute the DLT pipeline."""
        pipeline = dlt.pipeline(
            pipeline_name=dlt.config["sources.nyc_taxi.pipeline_name"],
            destination=dlt.config["sources.nyc_taxi.destination"],
            dataset_name=dlt.config["sources.nyc_taxi.dataset_name"],
        )

        load_info = pipeline.run(load_taxi_data(self))
        print("\n" + "=" * 60)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(load_info)
        return load_info


if __name__ == "__main__":
    pipeline = NYCTaxiDLTPipeline()
    pipeline.run_pipeline()
