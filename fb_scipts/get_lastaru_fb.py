
import datetime
import logging
import fdb

from errormail import *

logging.basicConfig(filename="log/logfile.log", encoding='utf-8', level=logging.INFO)

def getLastAruLaurel(fbHost, fbData, fbUser, fbPass):
  
    try:
        con = fdb.connect(
            host=fbHost, database=fbData,
            user=fbUser, password=fbPass 
        )
        cur = con.cursor()
        cur.execute("SELECT FIRST 1 ID FROM CIK ORDER BY ID DESC")
        for row in cur.fetchone():
            lastID = row

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " Error while connecting to Firebird-SQL " + f"{err}")
        errorMail(err)
        exit(1)

    return(lastID)