# BigData2026Exam

🚢 AIS Incidentų Detekcijos ir Analizės Platforma (v11.0.4)
Ši platforma skirta lygiagrečiam laivų pavojingų priartėjimų (incidentų) identifikavimui užduotoje zonoje, 
naudojant Apache Spark (PySpark) ir Docker. Sistema yra pilnai parametrizuota ir optimizuota veikti 
tiek galinguose serveriuose, tiek vidutinių parametrų kompiuteriuose.

🏗️ Architektūriniai Sprendimai
Kodėl naudoju Apache Parquet?
Ilgą laiką duomenų apdorojimui naudojau CSV failus, tačiau projektui augant, jie tapo „butelio kakliuku“. Perėjau prie Apache Parquet dėl šių priežasčių:
- Stulpelinis saugojimas (Columnar Storage): Skirtingai nei CSV, Parquet leidžia „Spark“ nuskaityti tik reikiamus stulpelius (pvz., tik koordinates), ignoruojant visą kitą duomenų masyvą.
- Efektyvus suspaudimas: Binarinis suspaudimas (Snappy, Gzip) sumažina duomenų apimtis nuo 50 GB iki 10–15 GB.
- Tipų saugojimas: Parquet išsaugo duomenų tipus (Integer, Double, Timestamp), todėl išvengiama papildomų cast operacijų skriptuose.

Kodėl ne MongoDB?
- Batch Processing optimizacija: kadangi „Spark“ yra pritaikytas Parquet failams, todėl išvengiu daug resursų reikalaujančių CSV failų skaitymo ir/arba JSON serializacijos.
- Resursų taupymas: Milijonai INSERT operacijų į MongoDB apkrautų sistemą labiau nei patys skaičiavimai (pirminis architektūrinis svarstymas)
- Paskirstymo paprastumas: Parquet failai gali būti saugomi tiesiog failų sistemoje be sudėtingo klasterio konfigūravimo.

Skaidymo (Partitioning) naudojimas
Naudoju partitionBy funkciją duomenų skaidymui pagal metus, mėnesį ir dieną. Tai leidžia išvengti „visos bazės skaitymo“ (Full Table Scan) – jei 
analizuojame konkrečią dieną, „Spark“ fiziškai neatidaro kitų dienų failų, taip pagreitinant analizę nuo dešimčių minučių iki kelių sekundžių.

📊 Projekto Architektūra ir Skriptų Eiga
Konvejeris susideda iš 5 izoliuotų etapų, kuriuos nuosekliai valdomi src/pipeline_runner.py pagalba:
Etapas	Skriptas	Paskirtis
0	script0_convert_to_parquet.py	CSV konvertavimas į optimizuotą Data Lake.
1	script1_preprocess.py	Geografinis filtravimas aplink Bornholmą.
2	script2_spark_encounter.py	Matematika grįsta Self-Join analizė.
3	script3_visualize.py	Interaktyvių Folium HTML žemėlapių generavimas.
4	script4_master_report.py	Jinja2 ataskaitų sugeneravimas.

⚙️ Aplinkos Valdymas
Visi sistemos parametrai yra centralizuoti project_config.env faile. Kodo keisti nereikia!
Pagrindiniai konfigūracijų žinynas:
1. Resursai ir Atminties Saugikliai
Parametras	Rekomendacija (PC / Serveris)	Aprašymas
SPARK_LOCAL_CORES	4 / 24	Branduolių skaičius (palikite 1-2 laisvus OS).
SPARK_LOCAL_DRIVER_MEMORY	8g / 64g	JVM atmintis (filtravimui ir Self-Join).
SPARK_LOCAL_OFFHEAP_SIZE	2g / 16g	PyArrow atmintis, apsauga nuo OOM klaidų.
2. Tinklas ir Paskirstyta (Distributed) Architektūra
Sistema išbandyta ir gali būti naudota tiek "local", tiek ir "distributed" režimu.

Parametras	Pavyzdys	Aprašymas
USING_DISTRIBUTED_CALCULATION	false / true	Ar jungtis prie išorinio klasterio.

Distributed režimu veikiančioje sistemoje būtina nurodyti šiuos parametrus:
USING_DISTRIBUTED_CALCULATION=false (pakeisti į true)
SPARK_MASTER_PUBLIC_IP=192.168.0.115 (pakeisti į savo MASTER IP adresą)
SPARK_MASTER_URL=spark://192.168.0.115:7077 (pakeisti į savo MASTER URL bei galimą portą)

Kritinis parametras macOS! Dėl to, kad "docker" MacOS aplinkoje veikia panaudojant linux vm, būtina nurodyti fizinį IP adresą.
Parametras	Pavyzdys	
SPARK_DRIVER_PHYSICAL_IP	192.168.1.124	


🛠️ Analizės našumo išvados:
Atlikęs testus su pusės metų (~526 GB) duomenų masyvu, nustačiau, kad Local režimas yra 27% efektyvesnis už Distributed režimą (užduoties vykdymo laikas ~50 vs ~70 min)
Pagrindinė priežastis – 1 Gbps tinklo pralaidumas, kuris tampa „butelio kakliuku“ duomenų skaitymo procese. 
Paskirstytas skaičiavimas yra rekomenduojamas esant šioms sąlymoms:
- pagrindinis darbas vyksta iš kompiuterio su limituotais skaičiavimo resursais, bet greita tinklo prieiga
- esant labai dideliems duomenų kiekiams
- kai vieno mazgo atminties (RAM) ir/arba diskų masyvo (HDD/SSD) ištekliai tampa ribojantys.

🛠️ Infrastruktūros skirtumai (Linux vs macOS)
Projekte naudojami atskiri docker-compose failai, nes operacinės sistemos skiriasi savo tinklo ir failų valdymo principais:
1. Tinklo architektūra: Linux naudoja network_mode: host tiesioginiam pasiekiamumui, macOS – bridge režimą su rankiniu IP nustatymu.
2. Laikinoji atmintis: Linux naudoja tmpfs (RAM diską) greičiui, macOS – named volume saugumui.
3. Saugumas: Fedora/Linux reikalauja :z vėliavėlės SELinux teisėms.
Individualaus paleidimo instrukcija:
• Linux: docker compose --env-file environment.common.env --env-file environment.linux.env -f docker-compose.linux.yml up --build
• macOS: docker compose --env-file environment.common.env --env-file environment.macos.env -f docker-compose.macos.yml up --build

🛠️ Sistemos paleidimas (Cross-Platform)
Projektas sukurtas taip, kad galėtų veikti vienodai tiek Linux (p510 serveris bei w520/w541 darbo stotys), tiek macOS (MacBook Air/Pro) aplinkose. 
Infrastruktūros orkestravimui naudojamas Makefile ir modulinių .env konfigūracijų sistema.

Pagrindinė paleidimo komanda:
	- make up

Pagrindinė kodo išjungimo komanda:
    - make down

Make komanda kartu su Makefile konfiguraciniu failu automatiškai atpažįsta operacinę sistemą ir parenka tinkamą Docker konfigūraciją bei aparatūros parametrus.
Kaip tai veikia?
1. OS aptikimas: Makefile automatiškai nustato kokia OS yra naudojama, t.y, ar dirbama su Darwin (macOS), ar Linux.
2. Modulinės konfigūracijos: Sistema įkrauna kintamuosius iš trijų sluoksnių: 
	- environment.common.env – bendri projekto parametrai (direktorijos, duomenų keliai). 
	- environment.linux.env arba environment.macos.env – aparatūrai specifiniai nustatymai (RAM, CPU branduoliai).
3. Konteinerizacija: Naudojami atskiri docker-compose.linux.yml ir docker-compose.macos.yml failai, kurie užtikrina, kad tinklo ir resursų nustatymai būtų pritaikyti konkrečiai sistemai.

Reikalavimai:
- Įdiegtas Docker (su docker compose v2+).
- Įdiegtas make įrankis (macOS įdiegtas pagal nutylėjimą, Linux – sudo apt/yum/dnf install make).

Patarimas: Prieš paleidžiant, visada užtikrinkite, kad nurodyti keliai fiziškai egzistuoja ir/arba pasiekiami jūsų kompiuteryje.
Keliai naudoti ruošiant v11.0.4 versiją naudoti tokie:
HOST_MASTER_PARQUET_DIR=../AIS_Task4_Analysis/AIS_DB.Parquet
HOST_INTERIM_DATA_DIR=../AIS_Task4_Analysis/Interim_Files
HOST_REPORTS_DATA_DIR=./Analysis_and_Reports
HOST_INPUT_DATA_DIR=/opt/A/NFS_Folder/TaskNr4/AIS_DB.CSV <- dėl didelės duomenų apimties (~53GB) duomenys saugomi centralizuotame NFS serveryje. 

Testavimui bandytas ir tinkle esantis NAS įrenginys, kuris pasiekiamas per 1Gbps tinklo prieigą bei naudojantis AFP, SMB arba NFSv3 tinklo protokolus. 
Naudotas NFSv3 protokolas leidžia vartotojams ir programoms pasiekti bei bendrinti failus nuotoliniame serveryje per kompiuterių tinklą tarp Linux ir MacOS taip, lyg jie būtų saugomi vietiniame kompiuterio diske.

🛠️ Sistemos paleidimas Read-Only režimu
Sistemą galima paleisti read-only režimu tais atvejais, kuomet CSV duomenys jau yra konvertuoti į Parquet formatą.
Tuo tikslu src/pipeline_runner.py išjungti "script0_convert_to_parquet.py" konvertavimo kodo veikimą.
Kodo veikimas tokiame modelyje buvo atliktas saugant duomenis NAS įrenginyje, kodą leidžiant Linux bei MacOS klientuose.
Kelias kur yra saugomi Parquet failai nurodytas taip:
HOST_MASTER_PARQUET_DIR=/Volumes/VolumeA/Tado_Mokslai/TaskNr4/AIS_DB.Parquet


📋 „Troubleshooting“ (Gedimų paieška)
• Problema: „Spark“ nepaleidžia konteinerio. • Patikrinkite, ar xxx.env failas yra projekto šaknyje. • Patikrinkite, ar nėra „invalid interpolation“ klaidų kelyje.
• Problema: Rezultatų ataskaita tuščia. • Patikrinkite TARGET_MAX_DISTANCE_NM – ar paieškos spindulys nėra per mažas.
• Problema: Tinklo klaidos ant macOS. • Patikrinkite SPARK_DRIVER_PHYSICAL_IP nustatymus environment.xxx.env failuose.

🚀 Perkeliamumas (Deployment)
Norint perkelti platformą į kitą kompiuterį naudotinos "docker save" / "docker load" komandos:
1. Sukurkite archyvą: docker save -o ais_analysis_v11.tar ais-analysis-task4-v11:v11.0.4 
2. Perkelkite failą ir įkelkite: docker load < ais_analysis_v11.tar

Dokumentacijos versija: v11.0.4 | Paskutinį kartą atnaujinta: 2026-06-04
