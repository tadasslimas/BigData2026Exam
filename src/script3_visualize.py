# -*- coding: utf-8 -*-
import os
import sys
import multiprocessing
from datetime import datetime, timedelta
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- 1. DINAMIŠKA KONFIGŪRACIJA IŠ .ENV ---
os.environ['PYSPARK_PYTHON'] = "python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python3"

driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "64g")
spark_cores = os.getenv("SPARK_LOCAL_CORES", "20")
spark_master = f"local[{spark_cores}]"

ENCOUNTERS_CSV = os.getenv("CONTAINER_REPORTS_PATH", "/app/data/Reports/detected_encounters.2021.12.csv")
INTERIM_DIRECTORY = os.getenv("CONTAINER_INTERIM_PATH", "/app/data/Interim_Files")
OUTPUT_BASE_DIR = str(Path(ENCOUNTERS_CSV).parent / "Danger_Events_Report")

# Pasiimame įvykių skaičiaus konfigūraciją
try:
    TOP_N_EVENTS = int(os.getenv("REPORT_TOP_N_EVENTS", "5"))
except (ValueError, TypeError):
    TOP_N_EVENTS = 5

try:
    import folium
    import matplotlib
    matplotlib.use('Agg')  
    import matplotlib.pyplot as plt
except ImportError:
    print("Klaida: Nerastos reikalingos bibliotekos. Įsidiekite: pip install folium matplotlib")
    raise

# --- 2. SPARK SESIJOS KŪRIMAS ---
spark = SparkSession.builder \
    .appName("AIS_Mass_Visualization_With_Plots.Docker") \
    .master(spark_master) \
    .config("spark.driver.memory", driver_memory) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.shuffle.partitions", spark_cores) \
    .getOrCreate()

print(f"=== SPARK 3 (VISUALIZATION) STARTAVO ===")
print(f"=== Rezervuoti branduoliai: {spark_cores} (Režimas: {spark_master}) ===")
print(f"=== Driver RAM skirta: {driver_memory} ===")
print(f"=== Ataskaitų šaltinis: {ENCOUNTERS_CSV} ===")
print(f"=== Grafinių įvykių išvesties aplankas: {OUTPUT_BASE_DIR} ===")


def get_speed_color(sog_val):
    try:
        sog = float(sog_val)
    except (ValueError, TypeError):
        return "blue"
    if sog <= 0.1: return "blue"
    elif sog <= 1.0: return "red"
    elif sog <= 5.0: return "orange"
    elif sog <= 12.0: return "yellow"
    else: return "green"


def generate_event_artifacts(args):
    idx, danger_score, mmsi1, mmsi2, dist_m, ts_str, top_n, output_base_folder, points_list = args

    v1_points = [p for p in points_list if p["mmsi"] == mmsi1]
    v2_points = [p for p in points_list if p["mmsi"] == mmsi2]

    if not v1_points or not v2_points:
        return f"[Top {idx}] Įspėjimas: Nepavyko rasti pakankamai trajektorijos taškų."

    v1_name = next((p["name"] for p in v1_points if p["name"]), f"MMSI {mmsi1}")
    v2_name = next((p["name"] for p in v2_points if p["name"]), f"MMSI {mmsi2}")

    try:
        # 🌟 PATAISYMAS 1: Kadangi datą saugome standartiniu ISO formatu iš Parquet, skaitome būtent jį
        tc_obj = datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S")
        clean_date = tc_obj.strftime("%Y%m%d_%H%M%S")
    except Exception:
        clean_date = "date_error"

    folder_name = f"Top_{idx}_Score_{danger_score}_{clean_date}_{mmsi1}_{mmsi2}"
    event_folder = Path(output_base_folder) / folder_name
    event_folder.mkdir(parents=True, exist_ok=True)

    # --- FOLIUM ŽEMĖLAPIS ---
    center_lat, center_lon = v1_points[len(v1_points) // 2]["coords"]
    m = folium.Map(location=[center_lat, center_lon], zoom_start=15, tiles="CartoDB.VoyagerLabelsUnder")

    legend_html = f"""
     <div style="position: fixed; bottom: 50px; left: 50px; width: 220px; height: 160px;
     border:2px solid grey; z-index:9999; font-size:14px; background-color:white; opacity: 0.9; padding: 10px;">
     <b>Rizikos balas: <span style="color:red;">{danger_score}</span></b><br>
     <b>Greitis (SOG):</b><br>
     <i style="background:green;width:12px;height:12px;float:left;margin-right:5px;border-radius:50%;"></i> &gt; 12 mzg<br>
     <i style="background:yellow;width:12px;height:12px;float:left;margin-right:5px;border-radius:50%;"></i> 5 - 12 mzg<br>
     <i style="background:orange;width:12px;height:12px;float:left;margin-right:5px;border-radius:50%;"></i> 1 - 5 mzg<br>
     <i style="background:red;width:12px;height:12px;float:left;margin-right:5px;border-radius:50%;"></i> 0.1 - 1 mzg<br>
     <i style="background:blue;width:12px;height:12px;float:left;margin-right:5px;border-radius:50%;"></i> &lt; 0.1 mzg<br>
     <br>
     <span style="color:grey;">--- {v1_name[:15]}</span><br>
     <span style="color:black;"><b>— {v2_name[:15]}</b></span>
     </div>
     """
    m.get_root().html.add_child(folium.Element(legend_html))

    def draw_line_and_markers(points, line_color, line_weight, name):
        coords = [p["coords"] for p in points]
        folium.PolyLine(coords, color=line_color, weight=line_weight, opacity=0.6, popup=name).add_to(m)
        for pt_idx, p in enumerate(points, 1):
            speed_color = get_speed_color(p["sog"])
            popup_txt = f"<b>Laivas:</b> {name}<br><b>Taškas:</b> #{pt_idx}<br><b>Laikas:</b> {p['ts_txt']}<br><b>Greitis:</b> {p['sog']} mzg"
            folium.CircleMarker(location=p["coords"], radius=6, color="black", weight=1, fill=True, fill_color=speed_color, fill_opacity=1.0, popup=folium.Popup(popup_txt, max_width=250)).add_to(m)

    draw_line_and_markers(v1_points, "grey", 3, v1_name)
    draw_line_and_markers(v2_points, "black", 5, v2_name)

    folium.Marker(location=v1_points[len(v1_points) // 2]["coords"], icon=folium.Icon(color="red" if danger_score >= 90 else "purple", icon="exclamation-sign"), popup=f"<b>KULMINACIJA</b><br>Atstumas: {dist_m} m").add_to(m)
    m.save(str(event_folder / "trajectory_map.html"))

    # --- MATPLOTLIB ---
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 12), sharex=False)
    fig.suptitle(f"Suartėjimo Analizė (Top {idx}, Danger Score: {danger_score})\nLaikas: {ts_str} | Atstumas: {dist_m} m", fontsize=14, fontweight='bold')

    # 🌟 PATAISYMAS 2: Taškų laiko formatai Matplotlib grafikui iš Parquet ISO formato
    v1_times = [datetime.strptime(p["ts_txt"].strip(), "%Y-%m-%d %H:%M:%S") for p in v1_points]
    v1_sog = [p["sog"] for p in v1_points]
    v1_cog = [p["cog"] for p in v1_points]

    v2_times = [datetime.strptime(p["ts_txt"].strip(), "%Y-%m-%d %H:%M:%S") for p in v2_points]
    v2_sog = [p["sog"] for p in v2_points]
    v2_cog = [p["cog"] for p in v2_points]

    # SOG Pokytis
    ax1.plot(v1_times, v1_sog, label=f"{v1_name} (MMSI {mmsi1})", color='grey', marker='o', linewidth=2)
    ax1.plot(v2_times, v2_sog, label=f"{v2_name} (MMSI {mmsi2})", color='black', marker='s', linewidth=2)
    ax1.axvline(x=tc_obj, color='red', linestyle='--', alpha=0.7, label='Kulminacijos momentas')
    ax1.set_ylabel("Greitis (SOG) mazgais", fontweight='bold')
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax1.legend(loc='upper right')
    ax1.set_title("Greičio dinamika laiko atžvilgiu")

    # COG Pokytis
    ax2.plot(v1_times, v1_cog, color='grey', marker='o', linewidth=2)
    ax2.plot(v2_times, v2_cog, color='black', marker='s', linewidth=2)
    ax2.axvline(x=tc_obj, color='red', linestyle='--', alpha=0.7)
    ax2.set_ylabel("Kursas (COG) laipsniais", fontweight='bold')
    ax2.set_xlabel("Laikas", fontweight='bold')
    ax2.set_ylim(0, 360)
    ax2.grid(True, linestyle=':', alpha=0.6)
    ax2.set_title("Kurso (krypties) pokyčiai")

    fig.autofmt_xdate()
    plt.tight_layout()

    plt.savefig(str(event_folder / "telemetry_charts.png"), dpi=150)
    plt.close(fig)

    return f"    [Sėkmė] Sukurtas aplankas: {folder_name} (Žemėlapis ir Grafikas paruošti)"


def mass_visualize_top_encounters_pyspark(encounters_csv, interim_folder, output_maps_folder, top_n=50):
    print(f"Nuskaitomi TOP {top_n} įvykiai pagal Danger_Score...")
    Path(output_maps_folder).mkdir(parents=True, exist_ok=True)

    df_encounters = spark.read.option("header", "true").option("inferSchema", "true").csv(encounters_csv)
    top_enc_list = df_encounters.orderBy(F.col("Danger_Score").desc()).limit(top_n).collect()
    print(f"Rasta įvykių analizės etapui: {len(top_enc_list)}")

    if not top_enc_list: return

    broadcast_data = []
    for idx, row in enumerate(top_enc_list, 1):
        try:
            # 🌟 SPRENDIMAS KLAIDOS NR2: Kadangi script2 galutiniame CSV laikas vadinasi TS_A ir yra ISO formato:
            tc_str = str(row["TS_A"]).strip()
            tc = datetime.strptime(tc_str, "%Y-%m-%d %H:%M:%S")
            window_start = tc - timedelta(minutes=10)
            window_end = tc + timedelta(minutes=10)
        except Exception as e:
            print(f"Klaida nuskaitant datą eilutėje {idx}: {e}")
            continue

        broadcast_data.append((
            int(idx), int(row["Danger_Score"]), str(row["MMSI_A"]).strip(), str(row["MMSI_B"]).strip(),
            float(row["Distance_NM"]), tc_str,
            window_start.strftime("%Y-%m-%d %H:%M:%S"), window_end.strftime("%Y-%m-%d %H:%M:%S")
        ))

    schema = StructType([
        StructField("event_idx", LongType(), False), StructField("Danger_Score", LongType(), False),
        StructField("MMSI_1", StringType(), True), StructField("MMSI_2", StringType(), True),
        StructField("Distance_Meters", DoubleType(), True), StructField("Timestamp", StringType(), True),
        StructField("Window_Start_Str", StringType(), True), StructField("Window_End_Str", StringType(), True)
    ])

    df_targets = spark.createDataFrame(broadcast_data, schema=schema)

    df_targets = df_targets \
        .withColumn("Window_Start", F.to_timestamp(F.col("Window_Start_Str"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("Window_End", F.to_timestamp(F.col("Window_End_Str"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("Window_Start_Str", "Window_End_Str")

    print("Nuskaitomi tarpiniai AIS duomenys...")
    #  SPRENDIMAS KRITINIAM LŪŽIUI: Skaitome tiesiai švarų Parquet failą, ne visą aplanką!
    df_interim = spark.read.parquet("/app/data/Interim_Files/AIS_Filtered.parquet")

    # 🌟 PATAISYMAS 3: Kadangi tipai Parquet bazėje jau idealūs, suvedame su jūsų kintamaisiais
    df_interim_prepared = df_interim \
        .withColumn("MMSI_str", F.col("MMSI").cast("string"))


    name_col = F.col("Name") if "Name" in df_interim_prepared.columns else F.lit("")

    print("Vykdoma lygiagreti trajektorijų paieška iš išvalytų eilučių...")
    # Kadangi neturime File_Source, jungiame tik pagal MMSI ir Laiko langą
    df_joined = df_interim_prepared.join(
        df_targets,
        ((df_interim_prepared["MMSI_str"] == df_targets["MMSI_1"]) | (df_interim_prepared["MMSI_str"] == df_targets["MMSI_2"])) &
        (df_interim_prepared["Parsed_TS"] >= df_targets["Window_Start"]) &
        (df_interim_prepared["Parsed_TS"] <= df_targets["Window_End"]),
        "inner"
    )


    raw_trajectories = df_joined.select(
        df_targets["event_idx"], df_targets["Danger_Score"], df_targets["MMSI_1"], df_targets["MMSI_2"],
        df_targets["Distance_Meters"], df_targets["Timestamp"].alias("Target_Timestamp"),
        df_interim_prepared["MMSI_str"], df_interim_prepared["Latitude"], df_interim_prepared["Longitude"],
        df_interim_prepared["Parsed_TS"].cast("string").alias("Interim_Timestamp"), df_interim_prepared["SOG"], df_interim_prepared["COG"],
        name_col.alias("Vessel_Name")
    ).collect()

    print(f"Duomenys surinkti (rasta {len(raw_trajectories)} taškų). Ruošiamos braižymo gijos...")
    events_map_data = {}

    for row in raw_trajectories:
        e_idx = row["event_idx"]
        if e_idx not in events_map_data:
            events_map_data[e_idx] = {
                "danger_score": row["Danger_Score"], "mmsi1": row["MMSI_1"], "mmsi2": row["MMSI_2"],
                "dist_m": row["Distance_Meters"], "ts_str": row["Target_Timestamp"], "points": []
            }

        sog_safe = float(row["SOG"]) if row["SOG"] is not None else 0.0
        cog_safe = float(row["COG"]) if row["COG"] is not None else 0.0

        events_map_data[e_idx]["points"].append({
            "mmsi": row["MMSI_str"],
            "coords": (float(row["Latitude"]), float(row["Longitude"])),
            "ts_txt": row["Interim_Timestamp"],
            "sog": sog_safe,
            "cog": cog_safe,
            "name": str(row["Vessel_Name"]).strip() if row["Vessel_Name"] else ""
        })

    tasks = []
    for e_idx, data in events_map_data.items():
        # Taškus rūšiuojame pagal ISO formatą
        data["points"].sort(key=lambda x: datetime.strptime(x["ts_txt"].strip(), "%Y-%m-%d %H:%M:%S"))
        tasks.append((
            e_idx, data["danger_score"], data["mmsi1"], data["mmsi2"], data["dist_m"], data["ts_str"],
            top_n, output_maps_folder, data["points"]
        ))

    if not tasks:
        print("Įspėjimas: Užduočių sąrašas tuščias, taškų generavimui nerasta.")
        return

    print(f"Pradedamas lygiagretus {len(tasks)} įvykių aplankų generavimas...")
    cores_to_use = max(1, int(spark_cores) - 2 if spark_cores != "*" else multiprocessing.cpu_count() - 2)

    with multiprocessing.Pool(processes=cores_to_use) as pool:
        results = pool.map(generate_event_artifacts, tasks)

    for res in results[:10]: print(res)
    if len(results) > 10: print(f"... ir dar {len(results) - 10} įvykių sugeneruota sėkmingai.")


if __name__ == "__main__":
    start_time = datetime.now()
    mass_visualize_top_encounters_pyspark(ENCOUNTERS_CSV, INTERIM_DIRECTORY, OUTPUT_BASE_DIR, top_n=TOP_N_EVENTS)
    spark.stop()
    print(f"\nUžduotis atlikta! Visi įvykių aplankai sugeneruoti per: {datetime.now() - start_time}")

