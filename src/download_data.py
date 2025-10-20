from datetime import datetime
from pathlib import Path

import requests


class NYCTaxiDataDownloader:
    def __init__(self, year: int):
        self.BASE_URL = (
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"
        )
        self.YEAR = year
        self.DATA_DIR = "data/raw/"

        Path(self.DATA_DIR).mkdir(parents=True, exist_ok=True)

    def get_file_path(self, month: int) -> Path:
        month_str = f"{month:02d}"
        file_name = f"yellow_tripdata_{self.YEAR}-{month_str}.parquet"
        return Path(self.DATA_DIR) / file_name

    def file_exists(self, month: int) -> bool:
        file_path = self.get_file_path(month)
        return file_path.exists()

    def download_month(self, month: int) -> bool:
        success = False
        if self.file_exists(month):
            print(
                f"⏭️ File for {self.YEAR}-{month:02d} already exists. Skipping download."
            )
            return True

        try:
            downloaded_file = requests.get(
                f"{self.BASE_URL}_{self.YEAR}-{month:02d}.parquet", stream=True
            )
            downloaded_file.raise_for_status()  # Raise an error for HTTP errors

            file_path = self.get_file_path(month)
            with open(file_path, "wb") as file:
                for chunk in downloaded_file.iter_content(chunk_size=8_192):
                    file.write(chunk)

            print(f"✅ Downloaded file for {self.YEAR}-{month:02d} successfully.")
            success = True
            return True

        except requests.RequestException as e:
            print(f"❌ Failed to download file for {self.YEAR}-{month:02d}: {e}")
            return False

        except Exception as e:
            print(
                f"❌ An error occurred while saving the file for {self.YEAR}-{month:02d}: {e}"
            )
            return False

        finally:
            # Cleanup in case of failure
            if not success and self.file_exists(month):
                self.get_file_path(month).unlink()  # Remove incomplete file

    def download_all_available(self) -> list:
        now = datetime.now()
        if self.YEAR != now.year:
            current_month = 12
        else:
            current_month = now.month
        downloaded_files = [
            self.get_file_path(month)
            for month in range(1, current_month + 1)
            if self.download_month(month)
        ]

        return downloaded_files


if __name__ == "__main__":
    downloader = NYCTaxiDataDownloader(year=2_024)
    files = downloader.download_all_available()
    print(f"\n📊 Summary: {len(files)} files processed")
