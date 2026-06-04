import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp

def csv_to_partitioned_parquet():
    print("\n========================================================")
    print(" 🚀 STARTUOJA KLASTERIO DUOMENŲ PARUOŠIMAS (PARQUET) ")
    print("========================================================")
    start_time = time.time()

    spark_cores = os.getenv("SPARK_LOCAL_CORES", "4") 
    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "8g") 
    code_version = os.getenv("CODE_VERSION", "v11_Parquet")
    offheap_size = os.getenv("SPARK_OFFHEAP_SIZE", "8g")

    csv_input = os.getenv("CONTAINER_INPUT_PATH", "/app/data/Input_Data")
    parquet_output = "/app/data/Master_Lake/AIS_Master.parquet"

    print(f" Resursai: local[{spark_cores}] | RAM: {driver_mem} + {offheap_size} OffHeap")
    print(f" Šaltinis (NFS CSV): {csv_input}")
    print(f" Tikslas (Vietinis Parquet): {parquet_output}")

    # --- 1. APLINKOS KINTAMŲJŲ SURINKIMAS ---
    USING_DISTRIBUTED_PYSPARK_CALCULATION = os.getenv("DISTRIBUTED_MODE", "false").lower() == "true"
    spark_master_ip = os.getenv("SPARK_MASTER_PUBLIC_IP", "192.168.0.115")

    # --- 2. SPARK SESIJOS KŪRIMAS ---
    spark_builder = SparkSession.builder \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.driver.memory", driver_mem) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", offheap_size)

    if USING_DISTRIBUTED_PYSPARK_CALCULATION:
    # SAUGIKLIS: Nurodome, kad tiek darbininkai, tiek driveris konteineryje naudotų standartinį python3
    # Šie kintamieji privalo būti nustatyti prieš sukuriant SparkSession
        os.environ['PYSPARK_PYTHON'] = "python3"
        os.environ['PYSPARK_DRIVER_PYTHON'] = "python3"

        spark = spark_builder \
            .appName(f"AIS_Process.Distributed.{code_version}") \
            .master(f"spark://{spark_master_ip}:7077") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.memory", "10g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.speculation", "true") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
            .getOrCreate()
    else:
        spark = spark_builder \
            .appName(f"AIS_Process.Local.{code_version}") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "24") \
            .getOrCreate()

    try:
        print("\n 1. Skaitomi tekstiniai CSV failai ...")
        df_raw = spark.read.option("header", "true").csv(csv_input)

        print(" - 1. Tvarkomas Timestamp stulpelio pavadinimas ...")
        if "# Timestamp" in df_raw.columns:
            df_raw = df_raw.withColumnRenamed("# Timestamp", "Timestamp")

        print(" - 2. Vykdomas stulpelių pervadinimas, tipų priskyrimas ir pjaustymas...")
        df_typed = df_raw \
            .withColumn("MMSI", col("MMSI").cast("integer")) \
            .withColumn("Latitude", col("Latitude").cast("double")) \
            .withColumn("Longitude", col("Longitude").cast("double")) \
            .withColumn("SOG", col("SOG").cast("double")) \
            .withColumn("COG", col("COG").cast("double")) \
            .withColumn("Parsed_TS", to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm:ss")) \
            .withColumn("Year", year(col("Parsed_TS"))) \
            .withColumn("Month", month(col("Parsed_TS"))) \
            .withColumn("Day", dayofmonth(col("Parsed_TS")))

        print(" - 3. Rašoma į vietinį diską naudojant Snappy suspaudimą ir PartitionBy...")
        df_typed.write \
            .mode("overwrite") \
            .partitionBy("Year", "Month", "Day") \
            .parquet(parquet_output)

        duration = time.time() - start_time
        print("\n========================================================")
        print(f" SĖKMĖ: CSV failas pilnai migruotas į Parquet su sutvarkytais stulpeliais!")
        print(f"  Sugaištas laikas: {round(duration / 60, 2)} min.")
        print("========================================================")

    except Exception as e:
        print(f"\n❌ Migracijos klaida: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    csv_to_partitioned_parquet()

