import sys
import datetime
from datetime import date, datetime, timedelta
import holidays
import logging
from dotenv import load_dotenv

from query_fb import *
from query_mysql import *
from insert_mysql import *
from writetocsv import *

load_dotenv()

logging.basicConfig(filename="log/logfile.log", encoding="utf-8", level=logging.INFO)

envBoltok = os.getenv("BOLTOK")

boltok = []
for bolt in envBoltok.split(","):
    boltok.append(bolt)

# check argv date is valid

### clear csv file after insert
def clearCsv(fileToClear):
    # opening the file with w+ mode truncates the file
    f = open(fileToClear, "w+")
    f.close()


### check if date is sunday or holiday
def dateWeekHoliday(yesterday):
    hu_holidays = holidays.HU()
    if yesterday in hu_holidays:
        return True
    if date.weekday(yesterday) == 6:
        return True
    return False


### validate date format befor using it
def validateDate(date_text):
    try:
        datetime.datetime.strptime(date_text, "%Y.%m.%d")
    except ValueError:
        logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S") + " dátum formátum: YYYY.MM.DD")
        exit(1)

def main():

    yesterday = date.today() - timedelta(days=1)

    # if we have argv
    if len(sys.argv) == 3:
        startDate = sys.argv[1]
        endDate = sys.argv[2]
        validateDate(startDate)
        validateDate(endDate)
        if startDate > endDate:
            logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S") + " A kezdő dátum nem lehet nagyobb mint a vég dátum")
            exit(1)
    else:
        startDate = yesterday.strftime("%Y.%m.%d")
        endDate = startDate

    # QUERYS:
    ## get last id for aru (boltok, fetchType, query script)
    lastIdFb = int(queryFb( boltok, "one", """ SELECT FIRST 1 ID FROM CIK ORDER BY ID DESC """))

    ## last CIKMNY id for ean
    lastCIKMNYFb = int(queryFb( boltok, "one", """ SELECT FIRST 1 ID FROM CIKMNY ORDER BY ID DESC """))

    ## last stored aru id in mysql for aru
    lastIdMysql = int(queryMysql( boltok, "one", """ SELECT max(arukod) FROM cikk"""))

    ## last stored CIKMNY id in mysql for ean
    lastCIKMNYMysql = int(queryMysql( boltok, "one", """ SELECT max(arukod_id) FROM ean"""))
    

    # INSERTS:
    ## get aru from firebird and pass to mysql
    if lastIdFb != lastIdMysql:
        getQuery = """ SELECT
                        CIK.ID, CIK.NEV, MNY.KOD, BSR.AFA_ID
                        FROM CIK
                        JOIN CIKADT ON CIK_ID=CIK.ID
                        JOIN BSR ON CIKADT.BSR_ID=BSR.ID
                        JOIN CIKMNY ON CIKMNY.CIK_ID=CIK.ID
                        JOIN MNY ON MNY.ID=CIKMNY.MNY_ID
                        WHERE CIK.ID BETWEEN %s AND %s AND KPC_ID = 1 """ % ( lastIdMysql, lastIdFb)

        aruToCsv = queryFb(boltok, "all", getQuery)
        writeToCsv(aruToCsv, "aru.csv")

        ## passing parameters number, csv file path, boltok, fetch all or one, query string. query string other half is in insert_mysql.py
        insertMysql(4, "aru.csv", boltok, "all", """INSERT INTO cikk (arukod, rovid_nev, me, afa_kod) VALUES (""" )

        clearCsv("aru.csv")

    ## get ean from firebird and pass to mysql
    if lastCIKMNYFb != lastCIKMNYMysql:
        getQuery = """  SELECT
                            CIK_ID, CIKMNY.ID, CIKKOD.KOD
                        FROM CIK
                        JOIN CIKMNY ON CIKMNY.CIK_ID = CIK."ID"  
                        JOIN CIKKOD ON CIKKOD.CIKMNY_ID = CIKMNY.ID
                        LEFT JOIN CIKKODKPC ON CIKKOD.KPC_ID  = CIKKODKPC.ID
                        WHERE CIKMNY.ID BETWEEN %s AND %s AND CIKKOD.KPC_ID = 3""" % ( lastCIKMNYMysql, lastCIKMNYFb)

        ##get result from sql
        eanToCsv = queryFb(boltok, "all", getQuery)
        ## write result to csv
        writeToCsv(eanToCsv, "ean.csv")

        ## passing parameters number, csv file path, boltok, fetch all or one, query string
        insertMysql( 3, "ean.csv", boltok, "all", """INSERT INTO ean (arukod_id, cikmny_id, ean_kod) VALUES (""" )

        clearCsv("ean.csv")


    if dateWeekHoliday(yesterday) == True:
        pass

    # TODO

    ## taltos
    ## getForgalom (startDate, endDate)
    ### forgalomToMysql ()


if __name__ == "__main__":
    main()
