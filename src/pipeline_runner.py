import os
import subprocess
import time
import sys

def run_script(script_name):
    print(f"\n========================================================")
    print(f"  STARTUOJA: {script_name}")
    print(f"========================================================")
    start_time = time.time()

    res = subprocess.run(["python3", f"src/{script_name}"])

    if res.returncode != 0:
        print(f" ❌ KLAIDA: {script_name} sugedo su kodu {res.returncode}!")
        # Vietoj exit() išmetame klaidą, kad suveiktų žemiau esantis 'finally' blokas
        raise RuntimeError(f"Skripto {script_name} avarija.")

    duration = time.time() - start_time
    print(f"  SĖKMINGAI BAIGTA: {script_name} (Trukmė: {round(duration, 2)} s.)")

if __name__ == "__main__":
    print("=== TaskNr4 AIS duomenų analizė PARQUET pagrindu ===")
    global_start = time.time()

    try:
        # 1. Pirmiausia atliekame binarinę migraciją (galima užkomentuoti, jei bazė jau sukurta)
        run_script("script0_convert_to_parquet.py")

        # 2. Vykdome greitą preprocess tiesiai iš vietinio Parquet
        run_script("script1_preprocess.py")

        # 3. Vykdome grynąją binarinę matematiką
        run_script("script2_spark_encounter.py")

        # 4. Vizualizacija ir ataskaitos
        run_script("script3_visualize.py")
        run_script("script4_master_report.py")

        print(f"\n========================================================")
        print(f" VISO PROCESO PABAIGA! Bendra trukmė: {round((time.time() - global_start) / 60, 2)} min.")
        print("========================================================")

    except Exception as e:
        print(f"\n🚨 PROCESAS NUTRAUKTAS: Konvejeris sustabdytas dėl klaidos.")
        sys.exit(1)

