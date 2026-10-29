# Fridrich Ferenc

## firebird data to MySql

---

## Description:

Copies daily data from Laurel (Firebird) to the BI MySQL database (`lazar`).
Written in python.

Master data: both databases' last ids are compared, the missing rows are
inserted to MySQL directly (`INSERT IGNORE` with column list, no temporary files).

master data tables:
- cikk (aru)
- ean
- taltos
- tiltas
- grill_besz

Receipt lines (BLOKK_TET) are **not copied**, only their daily totals:
- blokk_sum – main Laurel (FBHOST)
- trafik_blokk_sum – trafik Laurel (FBHOSTTRAF)
- grill – NP-* and K-* items, handover value from grill_besz

They are summed in Firebird. The last 7 days are recomputed every run in one
MySQL transaction, because lines can arrive a day late.

The BI app (`lazar-bi`) reads only these tables.

---

### **How to use:**

The Docker container keeps running, `scheduler.py` starts `main.py` every day at
`RUN_AT` (docker-compose.yml, local time Europe/Budapest, more times: `"06:00,13:00"`).

    docker compose up -d --build
    docker logs firebird_to_mysql        # next run, exit codes

Manual run:

    docker compose run --rm firebird_to_mysql python main.py

Recompute a date range (eg. missing days), format YYYY.MM.DD:

    docker compose run --rm firebird_to_mysql python main.py 2026.10.01 2026.10.31

First argument is the start date, the second is the end date.
Start date cannot be later than end date.

---

#### **Requirements:**

    pip3 install -r requirements.txt

Needs the Firebird client library (in the Docker image: `libfbclient2`).

---

### **Files and folders:**

- main.py – the daily run
- scheduler.py – keeps the container running, starts main.py at RUN_AT
- sum_mysql.py – daily totals from Laurel to blokk_sum, trafik_blokk_sum, grill
- query_fb.py, query_mysql.py – Firebird / MySQL queries
- insert_mysql.py – Firebird rows to MySQL (table, column list, rows)
- errormail.py – error mail
- dblog.py – log records to the shared applog database
- DOTEnv – .env template
- log – logfile.log

---

### **Log database (applog)**

Every logging record goes to the shared MySQL database `applog` as well
(`dblog.py`, LOGDB_* in .env, project name: LOG_PROJEKT in docker-compose.yml).
The file log stays; if applog is down, the run goes on.

- `applog.futas` – one row per scheduled run: projekt, kezdet, veg, kilepesi_kod, statusz (fut / ok / hiba)
- `applog.log` – log rows: ido, projekt, futas_id, szint, uzenet, reszletek (traceback), gep
- times are UTC (the MySQL server runs in UTC)
- user `applog`: SELECT, INSERT, UPDATE on applog.* – for every project and the viewer app

Test one run right now:

    docker compose run --rm firebird_to_mysql python scheduler.py --once

---

### **Database**

- at .env file (see DOTEnv)
- Laurel (Firebird): read only
- BI MySQL: 192.168.103.25, database `lazar`

---
