# Fridrich Ferenc

## firebird data to MySql

---

## Description:

Copies daily data from Laurel (Firebird) to the BI MySQL database (`lazar`).
Written in python.

Master data: both databases' last ids are compared, the missing rows are
written to a temporary csv, loaded to MySQL (`LOAD DATA`), then the csv is cleared.

master data tables:
- cikk (aru)
- ean
- taltos
- tiltas
- nomenklatura, cikk_csoport
- grill_besz, logisztika

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
- insert_mysql_bulk.py – csv to MySQL with `LOAD DATA` (column list when the table has an auto id)
- writetocsv.py – query result to csv (`\n` line end, as `LOAD DATA` expects)
- errormail.py – error mail
- insert_mysql.py – old row by row insert, not used
- DOTEnv – .env template
- csv – temporary csv files
- log – logfile.log

---

### **Database**

- at .env file (see DOTEnv)
- Laurel (Firebird): read only
- BI MySQL: 192.168.103.25, database `lazar`

---
