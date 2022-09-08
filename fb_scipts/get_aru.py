import csv
import datetime
import logging
import fdb
from dotenv import load_dotenv
import os

from errormail import *

logging.basicConfig(filename="log/logfile.log", encoding='utf-8', level=logging.INFO)

load_dotenv()

# enviroment variables setup
fbHost = os.getenv("FBHOST")
fbData = os.getenv("FBDATA")
fbUser = os.getenv("FBUSER")
fbPass = os.getenv("FBPASS")

envBoltok = os.getenv("BOLTOK")

boltok = []
for bolt in envBoltok.split(","):
    boltok.append(bolt)


def getAru (startDate, endDate):
    fileAru = open('aru_temp.csv', 'w', encoding="UTF8", newline='')
    writerAru = csv.writer(fileAru)
    try:
        con = fdb.connect(
            host=fbHost, database=fbData,
            user=fbUser, password=fbPass 
        )
        cur = con.cursor()
        for bolt in boltok:
            SELECT = f"SELECT SUM(bteny_ert), SUM(nteny_ert), SUM(nyilv_ert) FROM blokk_tet WHERE datum between '{startDate}' AND '{endDate}' AND egyseg = '{bolt}'"
            
            cur.execute(SELECT)
            for row in cur:
                writerAru.writerow([bolt] + [row[0]] + [row[1]] + [row[2]])


    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " Error while connecting to Firebird-SQL " + f"{err}")
        errorMail(err)
        exit(1)

    fileAru.close()