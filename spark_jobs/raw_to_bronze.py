from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import input_file_name, current_timestamp

from mapping import COLUMN_MAPPING, BRONZE_SCHEMA

def standardize_dataframe(df: DataFrame, column_mapping: dict) -> DataFrame:
    for col_name in df.columns:
        if col_name in column_mapping:
            df = df.withColumnRenamed(col_name, column_mapping[col_name])
    return df

def main():
    spark = SparkSession.builder \
        .appName("Raw to Bronze Robust ETL") \
        .remote("sc://localhost:15002") \
        .config("spark.cores.max", "4") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .getOrCreate()
        
    container_data_path = "/app/data"        
    raw_base = "/app/data/raw"
    bronze_base = "/app/data/bronze"
    
    print("Iniciando ingestão e padronização raw")            
    
    print("Lendo CSV...")
    df_csv_raw = spark.read.csv(
        f"{raw_base}/csv/*/*/*/*.csv", 
        header=True, 
        sep=";",
        inferSchema=True
    ).withColumn("source_file", input_file_name()) 
    
    print("Lendo JSON...")
    df_json_raw = spark.read.json(
        f"{raw_base}/json/*/*/*/*.json",
        multiLine=True
    ).withColumn("source_file", input_file_name())
    
    print("Lendo Parquet")
    df_parquet_raw = spark.read.schema(BRONZE_SCHEMA) \
        .parquet(f"{raw_base}/parquet/*/*/*/*.parquet"
    ).withColumn("source_file", input_file_name())
        
    print("Padronizando nomes de colunas")
    df_csv_std = standardize_dataframe(df_csv_raw, COLUMN_MAPPING)
    df_json_std = standardize_dataframe(df_json_raw, COLUMN_MAPPING)
    df_parquet_std = standardize_dataframe(df_parquet_raw, COLUMN_MAPPING)
        
    print("Consolidando dataframes")    
    df_consolidate = df_csv_std.unionByName(df_json_std, allowMissingColumns=True) \
                           .unionByName(df_parquet_std, allowMissingColumns=True)
                           
    for field in BRONZE_SCHEMA.fields:
        if field.name in df_consolidate.columns:
            df_consolidate = df_consolidate.withColumn(field.name, F.col(field.name).cast(field.dataType))
                           
    
    df_final = df_consolidate \
                    .withColumn("ingestion_date", F.current_timestamp()) \
                    .withColumn("session_start_ts", (F.col("session_start") / F.lit(1e6)).cast("timestamp")) \
                    .withColumn("year", F.year("session_start_ts")) \
                    .withColumn("month", F.month("session_start_ts")) \
                    .withColumn("day", F.dayofmonth("session_start_ts"))

    df_final.printSchema()                       

    df_final.show(5, truncate=False)
   
    df_final.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(str(bronze_base))
            
    print("Dados consolidados e salvos com sucesso!")
    

if __name__ == "__main__":
    main()