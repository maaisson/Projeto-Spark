from pyspark.sql import SparkSession, functions as F
from pathlib import Path
from config import settings

spark = SparkSession.builder \
    .appName("RawToBronze") \
    .remote("sc://localhost:15002") \
    .config("spark.ui.showConsoleProgress", "true") \
    .getOrCreate()
    

df_csv = spark.read.csv(str(settings.RAW_DATA_PATH / "*.csv"), header=True, inferSchema=True)           
df_parquet = spark.read.parquet(str(settings.RAW_DATA_PATH) / "*.parquet")
df_json = spark.read.json(str(settings.RAW_DATA_PATH) / "*.json")

print("===CSV===")
df_csv.show(6)
print("===PARQUET===")
df_parquet.show(6)
print("===JSON===")
df_json.show(6)

