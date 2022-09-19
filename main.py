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
        logging.error(
            " "
            + datetime.now().strftime("%Y.%m.%d %H:%M:%S")
            + " dátum formátum: YYYY.MM.DD"
        )
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
            logging.error(
                " "
                + datetime.now().strftime("%Y.%m.%d %H:%M:%S")
                + " A kezdő dátum nem lehet nagyobb mint a vég dátum"
            )
            exit(1)
    else:

        startDate = yesterday.strftime("%Y.%m.%d")
        endDate = startDate

    # QUERYS:
    # get last id for aru (query script, boltok, fetchType)
    lastIdFb = int(
        queryFb(
            boltok,
            "one",
            """ 
                SELECT
                  FIRST 1 ID FROM CIK
                ORDER BY ID DESC """,
        )
    )

    lastIdMysql = int(
        queryMysql(
            boltok,
            "one",
            """
                                 SELECT
                                 max(arukod) 
                                 FROM cikk""",
        )
    )

    if lastIdFb != lastIdMysql:
        getQuery = """ SELECT
                        CIK.ID, CIK.NEV, MNY.KOD, AFA.NEV
                        FROM CIK
                        JOIN CIKADT ON CIK_ID=CIK.ID
                        JOIN BSR ON CIKADT.BSR_ID=BSR.ID
                        JOIN AFA ON BSR.AFA_ID=AFA.ID
                        JOIN CIKMNY ON CIKMNY.CIK_ID=CIK.ID
                        JOIN MNY ON MNY.ID=CIKMNY.MNY_ID
                        WHERE CIK.ID BETWEEN %s AND %s AND KPC_ID = 1 """ % (
            lastIdMysql,
            lastIdFb,
        )

        aruToCsv = queryFb(boltok, "all", getQuery)
        writeToCsv(aruToCsv, "aru.csv")

        # Insert
        ## passing parameters number, csv file path, boltok, fetch all or one, query string
        insertMysql(
            4,
            "aru.csv",
            boltok,
            "all",
            """INSERT INTO cikk (arukod, rovid_nev, me, Afa) "VALUES (""",
        )

        clearCsv("aru.csv")

    if dateWeekHoliday(yesterday) == True:
        pass

    # TODO

    # aruToMysql(mysqlHost, mysqlData, mysqlUser, mysqlPass) csv -> to  mysql
    ##getForgalom (startDate, endDate)
    ### forgalomToMysql ()


if __name__ == "__main__":
    main()
