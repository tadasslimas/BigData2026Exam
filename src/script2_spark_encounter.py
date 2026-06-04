import os
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def run_encounter_mathematics():
    print("\n========================================================")
    print("  STARTUOJA: script2_spark_encounter.py (Parquet Versija) ")
    print("========================================================")
    start_time = time.time()

    spark_cores = os.getenv("SPARK_LOCAL_CORES", "20")
    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "64g")
    offheap_size = os.getenv("SPARK_OFFHEAP_SIZE", "16g")

    interim_input = "/app/data/Interim_Files/AIS_Filtered.parquet"
    final_csv_output = os.getenv("CONTAINER_REPORTS_PATH", "/app/data/Reports/detected_encounters.2021.12.csv")

    spark = SparkSession.builder \
        .appName("AIS_Math_Parquet.v11") \
        .master(f"local[{spark_cores}]") \
        .config("spark.driver.memory", driver_mem) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", offheap_size) \
        .config("spark.sql.shuffle.partitions", spark_cores) \
        .getOrCreate()

    try:
        print(f" Įkraunami binariniai duomenys iš analizuojamos zonos: {interim_input}")
        df_raw = spark.read.parquet(interim_input)

        #  1. SPECIALIŲJŲ LAIVŲ VALYMAS 
        if "Name" in df_raw.columns:
            print(" Nenaudojami specialieji laivai (PILOT, RESCUE, SEARCH, KBV)...")
            name_upper = F.upper(F.col("Name"))
            df_raw = df_raw.filter(
                ~name_upper.contains("PILOT") &
                ~name_upper.contains("RESCUE") &
                ~name_upper.contains("SEARCH") &
                ~name_upper.contains("KBV")
            )

        # 🌟 2. SOG IR COG HISTORIJOS SEKIMAS 
        print(" Vykdomas istorijos sekimas naudojant Window funkcijas...")
        vessel_history_window = Window.partitionBy("MMSI_str").orderBy("Parsed_TS")

        df_with_history = df_raw \
            .withColumn("Prev_SOG", F.lag("SOG", 1).over(vessel_history_window)) \
            .withColumn("Prev_COG", F.lag("COG", 1).over(vessel_history_window)) \
            .withColumn("Prev_TS", F.lag("Parsed_TS", 1).over(vessel_history_window)) \
            .withColumn("Next_TS", F.lead("Parsed_TS", 1).over(vessel_history_window))

        # Pokyčių skaičiavimas
        df_with_history = df_with_history.withColumn(
            "Time_Delta_Sec", F.unix_timestamp("Parsed_TS") - F.unix_timestamp("Prev_TS")
        ).withColumn(
            "Delta_SOG", F.round(F.col("SOG") - F.col("Prev_SOG"), 2)
        )

        # COG pjaustymas įvertinant 360 laipsnių ribą
        raw_cog_diff = F.abs(F.col("COG") - F.col("Prev_COG"))
        df_with_history = df_with_history.withColumn(
            "Delta_COG",
            F.round(F.when(raw_cog_diff.isNull(), 0.0)
            .otherwise(F.when(raw_cog_diff > 180.0, 360.0 - raw_cog_diff).otherwise(raw_cog_diff)), 2)
        )

        df_with_history = df_with_history.withColumn("Is_Last_Signal", F.when(F.col("Next_TS").isNull(), 1).otherwise(0))

        #  3. PATIKIMUMO IR MINIMALAUS GREIČIO FILTRAI 
        print(" Filtruojami tik judantys (2.0 >= SOG <= 45) ir aktyvūs laivai...")
        mmsi_counts = df_with_history.groupBy("MMSI_str").count().filter("count >= 10")

        # 🌟 SPRENDIMAS: Leidžiame tik tikrų laivų greičius (nuo 2.0 iki 45.0 mazgų)
        # Viskas, kas juda greičiau (malūnsparniai, anomalijos), iškart išmetama iš analizės!
        df_filtered = df_with_history \
            .filter(
                F.col("MMSI_str").isNotNull() &
                (F.col("SOG") >= 2.0) &
                (F.col("SOG") <= 45.0) &
                F.col("Parsed_TS").isNotNull()
            ) \
            .join(mmsi_counts, "MMSI_str", "inner")


        # Laiko pjaustymas į 10 sekundžių laiko tarpus sinchronizacijai
        df_with_time = df_filtered \
            .withColumn("Epoch", F.unix_timestamp("Parsed_TS")) \
            .withColumn("Time_Bucket", F.from_unixtime((F.col("Epoch") / 10).cast("long") * 10))

        # 🌟 4. LAIVŲ PORŲ INTERSEKCIJA (SELF-JOIN)
        print("🔗 Jungiamos laivų poros laiko tarpuose ...")
        v1 = df_with_time.alias("v1")
        v2 = df_with_time.alias("v2")

        encounters = v1.join(
            v2,
            (F.col("v1.Time_Bucket") == F.col("v2.Time_Bucket")) &
            (F.col("v1.File_Source") == F.col("v2.File_Source")) &
            (F.col("v1.MMSI_str") < F.col("v2.MMSI_str")),
            "inner"
        )

        # 🌟 5. TIKSLAUS ATSTUMO SKAIČIAVIMAS NAUDOJANT ATAN2 
        print(" Skaičiuojama tiksli trigonometrinė Haversine distancija tarp porų...")
        earth_radius_nm = 3440.065

        encounters_with_dist = encounters.withColumn(
            "haversine_a",
            F.sin(F.radians(F.col("v2.Latitude") - F.col("v1.Latitude")) / F.lit(2)) ** 2 +
            F.cos(F.radians(F.col("v1.Latitude"))) * F.cos(F.radians(F.col("v2.Latitude"))) *
            F.sin(F.radians(F.col("v2.Longitude") - F.col("v1.Longitude")) / F.lit(2)) ** 2
        ).withColumn(
            "Distance_NM",
            F.lit(2) * F.atan2(F.sqrt(F.col("haversine_a")), F.sqrt(F.lit(1) - F.col("haversine_a"))) * F.lit(earth_radius_nm)
        ).drop("haversine_a")

        # Filtruojame prasilenkimus, kurie yra arti (<= 0.054 NM)
        encounters_filtered = encounters_with_dist.filter(F.col("Distance_NM") <= 0.054) \
            .withColumn("Distance_Meters", F.round(F.col("Distance_NM") * 1852, 1)) \
            .withColumn("Avg_Lat", F.round((F.col("v1.Latitude") + F.col("v2.Latitude")) / 2, 6)) \
            .withColumn("Avg_Lon", F.round((F.col("v1.Longitude") + F.col("v2.Longitude")) / 2, 6))

        # Išlaikome tik patį artimiausią šios konkrečios poros prasilenkimo momentą
        window_spec = Window.partitionBy("v1.File_Source", "v1.MMSI_str", "v2.MMSI_str").orderBy("Distance_NM")
        df_best_encounters = encounters_filtered.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

        # 🌟 6. Skaičiuojama BALŲ MATRICA 
        print(" Skaičiuojama pavojaus balų matrica...")
        final_records = df_best_encounters.withColumn(
            "Danger_Score",
            F.when(F.col("Distance_Meters") <= 20.0, 50)
             .when(F.col("Distance_Meters") <= 50.0, 30)
             .otherwise(10) +
            F.when((F.col("v1.SOG") >= 3.0) & (F.col("v2.SOG") >= 3.0),
                F.when((F.col("v1.Delta_COG") > 30.0) | (F.col("v2.Delta_COG") > 30.0), 40)
                 .when((F.col("v1.Delta_COG") > 15.0) | (F.col("v2.Delta_COG") > 15.0), 20)
                 .otherwise(0)
            ).otherwise(0) +
            F.when((F.col("v1.Delta_SOG") <= -2.0) | (F.col("v2.Delta_SOG") <= -2.0), 30)
             .when((F.col("v1.Delta_SOG") >= 2.0) | (F.col("v2.Delta_SOG") >= 2.0), 15)
             .otherwise(0) -
            F.when(F.col("v1.Time_Delta_Sec") > 120, 30).otherwise(0) +
            F.when((F.col("v1.Is_Last_Signal") == 1) | (F.col("v2.Is_Last_Signal") == 1), 100).otherwise(0)
        )

        # Rūšiuojame pagal pavojaus lygį ir formuojame galutinius stulpelius
        final_records_sorted = final_records.orderBy(F.col("Danger_Score").desc()) \
            .select(
                F.col("Danger_Score"),
                F.col("v1.MMSI_str").alias("MMSI_A"),
                F.col("v2.MMSI_str").alias("MMSI_B"),
                F.col("Distance_NM"),
                F.col("Distance_Meters"),
                F.date_format(F.col("v1.Parsed_TS"), "yyyy-MM-dd HH:mm:ss").alias("TS_A"),
                F.col("Avg_Lat").alias("Latitude"),
                F.col("Avg_Lon").alias("Longitude"),
                F.col("v1.SOG").alias("SOG_A"),
                F.col("v2.SOG").alias("SOG_B"),
                F.col("v1.Delta_SOG").alias("Delta_SOG_1"),
                F.col("v2.Delta_SOG").alias("Delta_SOG_2"),
                F.col("v1.Delta_COG").alias("Delta_COG_1"),
                F.col("v2.Delta_COG").alias("Delta_COG_2"),
                F.col("v1.Time_Delta_Sec").alias("Report_Interval_Sec_1"),
                F.col("v1.File_Source").alias("File_Source")
            )

        print("\n========================================================")
        print("PROCESAS BAIGTAS, RENKAMA STATISTIKA...")
        absolute_max_danger = final_records_sorted.first()
        if absolute_max_danger:
            print(f"🔥 Maksimalus užfiksuotas pavojaus balas: {absolute_max_danger['Danger_Score']} balų!")
            print(f"Laivai lyderiai: {absolute_max_danger['MMSI_A']} ir {absolute_max_danger['MMSI_B']}")
        print("========================================================")

        # Suformuojame galutinį CSV per laikiną katalogą, kaip tavo reference
        target_file = Path(final_csv_output)
        target_file.parent.mkdir(parents=True, exist_ok=True)
        spark_part_folder = target_file.parent / (target_file.name + "_spark_part")

        final_records_sorted.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(str(spark_part_folder))

        actual_csv = list(spark_part_folder.glob("part-*.csv"))[0]
        actual_csv.replace(target_file)

        for f in spark_part_folder.iterdir(): f.unlink()
        spark_part_folder.rmdir()
        print(f" Galutinė ataskaita sėkmingai suformuota: {target_file}")

    except Exception as e:
        print(f"\n❌ Matematikos klaida: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_encounter_mathematics()

