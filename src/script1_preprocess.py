import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, radians, sin, cos, sqrt, atan2, lit, element_at, split, input_file_name

def process_parquet_partitions():
    print("\n========================================================")
    print(" STARTUOJA: script1_preprocess.py ")
    print("========================================================")
    start_time = time.time()

    spark_cores = os.getenv("SPARK_LOCAL_CORES", "20")
    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "64g")
    offheap_size = os.getenv("SPARK_OFFHEAP_SIZE", "16g")

    parquet_input = "/app/data/Master_Lake/AIS_Master.parquet"
    interim_output = "/app/data/Interim_Files/AIS_Filtered.parquet"

    spark = SparkSession.builder \
        .appName("AIS_Preprocess_Parquet") \
        .master(f"local[{spark_cores}]") \
        .config("spark.driver.memory", driver_mem) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", offheap_size) \
        .getOrCreate()

    try:
        print(f" Nuskaitoma vietinė master Parquet bazė: {parquet_input}")
        df_base = spark.read.parquet(parquet_input)

        # SPRENDIMAS: Dinamiškai sugeneruojame script2 reikalingus stulpelius čia pat vietoje!
        df_base = df_base \
            .withColumn("MMSI_str", col("MMSI").cast("string")) \
            .withColumn("File_Source", element_at(split(input_file_name(), "/"), -1))

        lat_center = float(os.getenv("AIS_CENTER_LAT", "55.225"))
        lon_center = float(os.getenv("AIS_CENTER_LON", "14.245"))
        max_dist_nm = float(os.getenv("AIS_MAX_DIST_NM", "50.0"))
        earth_radius_nm = 3440.065

        print(f" Taikoma Haversine formulė ({max_dist_nm} NM)...")
        
        # Tikslusis apskritimo filtravimas pagal reference matematikos modelį
        df_with_dist = df_base.withColumn(
            "Dist_From_Center",
            lit(2) * atan2(
                sqrt(
                    sin(radians(col("Latitude") - lit(lat_center)) / lit(2)) ** 2 +
                    cos(radians(lit(lat_center))) * cos(radians(col("Latitude"))) *
                    sin(radians(col("Longitude") - lit(lon_center)) / lit(2)) ** 2
                ),
                sqrt(
                    lit(1) - (
                        sin(radians(col("Latitude") - lit(lat_center)) / lit(2)) ** 2 +
                        cos(radians(lit(lat_center))) * cos(radians(col("Latitude"))) *
                        sin(radians(col("Longitude") - lit(lon_center)) / lit(2)) ** 2
                    )
                )
            ) * lit(earth_radius_nm)
        )

        df_filtered = df_with_dist.filter(col("Dist_From_Center") <= max_dist_nm)

        print(" Rašomi griežtai išvalyti binariniai duomenys tolimesnei matematikai...")
        # Saugiai išsaugome absoliučiai viską, ko reikės script2 sekimui ir script3 braižymui
        df_filtered.select(
            "Parsed_TS", "MMSI", "MMSI_str", "Latitude", "Longitude", 
            "Navigational status", "ROT", "SOG", "COG", "Heading", "IMO", "Callsign", 
            "Name", "Ship type", "Cargo type", "Width", "Length", "File_Source"
        ).write \
            .mode("overwrite") \
            .parquet(interim_output)

        duration = time.time() - start_time
        print("========================================================")
        print(f" PREPROCESS ETAPAS BAIGTAS SĖKMINGAI!")
        print(f"  Trukmė: {round(duration, 2)} s.")
        print("========================================================")

    except Exception as e:
        print(f"\n❌ Preprocess klaida: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_parquet_partitions()


