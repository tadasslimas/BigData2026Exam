# -*- coding: utf-8 -*-
import os
import csv
from pathlib import Path
from datetime import datetime

# --- 1. DINAMIŠKA KONFIGŪRACIJA IŠ .ENV VALDYMO PULTO ---
ENCOUNTERS_CSV = os.getenv("CONTAINER_REPORTS_PATH", "/app/data/Reports/detected_encounters.2021.12.csv")

# Pasiimame įvykių skaičiaus konfigūraciją
try:
    TOP_N_EVENTS = int(os.getenv("REPORT_TOP_N_EVENTS", "5"))
except (ValueError, TypeError):
    TOP_N_EVENTS = 5

# Pasiimame ataskaitos antraštę
REPORT_TITLE = os.getenv("REPORT_MASTER_TITLE", "AIS Incidentų Suvestinė")

# Pasiimame HTML failo pavadinimą iš .env su saugiu defaultu
HTML_FILENAME = os.getenv("REPORT_HTML_FILENAME", "Encounter_Master_Report.html")

# Sufatūruojame kelius reliatyviai pagal gautą aplinką
REPORTS_DIR = Path(ENCOUNTERS_CSV).parent
DANGER_EVENTS_DIR = REPORTS_DIR / "Danger_Events_Report"

# ČIA INTEGRUOJAMAS DINAMIŠKAS PAINTERS KELIAS:
MASTER_REPORT_HTML = REPORTS_DIR / HTML_FILENAME

def generate_html_report():
    print(f"\n========================================================")
    print(f" 🚀 STARTUOJA: script4_master_report.py (Parquet Versija)")
    print(f"========================================================")
    print(f"=== Konfigūracija: Rodysime TOP {TOP_N_EVENTS} įvykių")
    print(f"=== Ataskaitos pavadinimas: '{REPORT_TITLE}'")
    print(f"=== Generuojamas failas: {HTML_FILENAME}")

    if not Path(ENCOUNTERS_CSV).exists():
        print(f"❌ KLAIDA: Nerastas pradinio analizės rezultato failas!")
        return

    # Nuskaitome CSV duomenis naudojant gamyklinį csv.DictReader
    top_rows = []
    total_incidents = 0
    with open(ENCOUNTERS_CSV, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_incidents += 1
            if len(top_rows) < TOP_N_EVENTS:
                top_rows.append(row)

    print(f"Sėkmingai nuskaityta incidentų bazė. Surasta įvykių: {total_incidents}. Generuojamas vaizdas...")

    # Pradedame HTML struktūrą su dinamišku pavadinimu
    html_content = f"""
    <!DOCTYPE html>
    <html lang="lt">
    <head>
        <meta charset="UTF-8">
        <title>{REPORT_TITLE}</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{ background-color: #f8f9fa; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
            .hero-section {{ background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); color: white; padding: 40px 20px; border-radius: 0 0 20px 20px; margin-bottom: 30px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
            .card {{ border: none; border-radius: 15px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); margin-bottom: 25px; transition: transform 0.2s; }}
            .card:hover {{ transform: translateY(-2px); box-shadow: 0 6px 18px rgba(0,0,0,0.1); }}
            .card-header {{ background-color: #ffffff; border-bottom: 2px solid #edf2f7; border-radius: 15px 15px 0 0 !important; padding: 15px 20px; }}
            .badge-danger {{ background-color: #dc3545; color: white; font-size: 1rem; padding: 8px 12px; border-radius: 8px; }}
            .artifact-frame {{ border: 1px solid #e2e8f0; border-radius: 12px; width: 100%; height: 550px; background-color: #fff; }}
            .img-fluid {{ border-radius: 12px; border: 1px solid #e2e8f0; }}
            .meta-box {{ background-color: #f1f5f9; padding: 12px; border-radius: 10px; font-size: 0.95rem; }}
        </style>
    </head>
    <body>

    <div class="container">
        <div class="hero-section text-center">
            <h1 class="display-5 fw-bold">{REPORT_TITLE}</h1>
            <p class="lead">Automatiškai sugeneruota PySpark konvejerio ataskaita • {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
            <span class="badge bg-light text-dark px-3 py-2">Išanalizuota incidentų iš viso: {total_incidents}</span>
        </div>

        <h2 class="mb-4 fw-bold text-secondary">🔥 TOP {len(top_rows)} Pavojingiausi Prasilenkimai</h2>
    """

    for idx, row in enumerate(top_rows, 1):
        score = row["Danger_Score"]
        # 🌟 SPRENDIMAS: Keičiame senus raktus į naujuosius iš script2 išvesties
        mmsi1 = row["MMSI_A"]
        mmsi2 = row["MMSI_B"]
        dist_nm = float(row["Distance_NM"])
        dist_m = round(dist_nm * 1852.0, 1)  # Konvertuojame jūrmiles atgal į metrus gražiam HTML atvaizdavimui
        ts_str = str(row["TS_A"]).strip()

        try:
            # 🌟 PATAISYMAS: Kadangi script2 įrašo datą ISO formatu iš Parquet:
            tc_obj = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            clean_date = tc_obj.strftime("%Y%m%d_%H%M%S")
        except:
            clean_date = "date_error"

        matching_folders = list(DANGER_EVENTS_DIR.glob(f"Top_{idx}_Score_{score}_*"))

        if matching_folders:
            folder_name = matching_folders[0].name
            map_path = f"Danger_Events_Report/{folder_name}/trajectory_map.html"
            chart_path = f"Danger_Events_Report/{folder_name}/telemetry_charts.png"
        else:
            print(f"⚠️ Įspėjimas: Nepavyko rasti vizualizacijų aplanko įvykiui Top {idx}")
            continue

        html_content += f"""
        <div class="card">
            <div class="card-header d-flex justify-content-between align-items-center">
                <h3 class="h5 mb-0 fw-bold text-dark">#{idx} Įvykis: Laivų {mmsi1} ir {mmsi2} suartėjimas</h3>
                <span class="badge badge-danger">Danger Score: {score} balų</span>
            </div>
            <div class="card-body p-4">
                <div class="row meta-box mb-4 text-center g-2">
                    <div class="col-md-3"><b>📅 Kulminacijos laikas:</b><br>{ts_str}</div>
                    <div class="col-md-3"><b>📏 Mažiausias atstumas:</b><br><span class="text-danger fw-bold">{dist_m} metrų ({round(dist_nm, 4)} NM)</span></div>
                    <div class="col-md-3"><b>🚢 SOG (Greitis):</b><br>L1: {row['SOG_A']} mzg | L2: {row['SOG_B']} mzg</div>
                    <div class="col-md-3"><b>📂 Duomenų šaltinis:</b><br><small class="text-muted">Binarinis Parquet ežeras</small></div>
                </div>

                <div class="row">
                    <div class="col-xl-6 mb-3">
                        <h4 class="h6 fw-bold text-muted mb-2">🌐 Interaktyvus trajektorijų žemėlapis</h4>
                        <iframe class="artifact-frame" src="{map_path}"></iframe>
                    </div>
                    <div class="col-xl-6 mb-3">
                        <h4 class="h6 fw-bold text-muted mb-2">📊 Greičio (SOG) ir Kurso (COG) dinamika</h4>
                        <img class="img-fluid w-100 object-fit-cover" src="{chart_path}" alt="Telemetrijos grafikai" style="height: 550px;">
                    </div>
                </div>
            </div>
        </div>
        """

    html_content += """
    </div>
    <footer class="text-center text-muted py-4 mt-5 bg-white border-top">
        <small>© AIS Analytics Engine • Powered by PySpark & Docker - by Tadas Šlimas</small>
    </footer>
    </body>
    </html>
    """

    with open(MASTER_REPORT_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"🎉 SĖKMĖ: Galutinė Master ataskaita sėkmingai sugeneruota!")
    print(f"👉 Failas išsaugotas kaip: {MASTER_REPORT_HTML}")
    print(f"========================================================\n")

if __name__ == "__main__":
    generate_html_report()

