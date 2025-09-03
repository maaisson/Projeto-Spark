import os
from datetime import datetime
from pathlib import Path

BASE_PATH = Path(__file__).resolve().parents[1]
DATA_PATH = BASE_PATH / "data"
RAW_PATH = DATA_PATH / "raw"
BRONZE_PATH = DATA_PATH / "bronze"
SILVER_PATH = DATA_PATH / "silver"
GOLD_PATH = DATA_PATH / "gold"

N_RECORDS_MIN = 100
N_RECORDS_MAX = 5000
DATE_START = datetime(2022, 1, 1)
DATE_END = datetime(2025, 12, 31)
