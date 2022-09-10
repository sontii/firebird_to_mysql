import csv
import datetime
import logging
import fdb

from errormail import *

logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)


def getAru(fbHost, fbData, fbUser, fbPass, lastIdFb, lastIdMysql):

    fileAru = open('aru_temp.csv', 'w', encoding="UTF8", newline='')
    writerAru = csv.writer(fileAru)
    try:
        con = fdb.connect(
            host=fbHost, database=fbData,
            user=fbUser, password=fbPass
        )
        cur = con.cursor()

        SELECT = f"SELECT "

        cur.execute(SELECT)
        for row in cur:
            writerAru.writerow(row)

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                      " Error while connecting to Firebird-SQL " + f"{err}")
        errorMail(err)
        exit(1)

    fileAru.close()
