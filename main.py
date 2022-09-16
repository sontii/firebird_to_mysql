import sys
import datetime
from datetime import date, timedelta
import logging
from dotenv import load_dotenv

from query_fb import *
from query_mysql import *

load_dotenv()

logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)

envBoltok = os.getenv("BOLTOK")

boltok = []
for bolt in envBoltok.split(","):
    boltok.append(bolt)

# check argv date is valid


def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y.%m.%d')
    except ValueError:
        logging.error(" " + datetime.datetime.now().strftime(
            '%Y.%m.%d %H:%M:%S') + " dátum formátum: YYYY.MM.DD")
        exit(1)


def main():

    yesterday = date.today() - timedelta(days=1)

    # if we have argv
    if len(sys.argv) == 3:
        startDate = sys.argv[1]
        endDate = sys.argv[2]
        validate(startDate)
        validate(endDate)
        if startDate > endDate:
            logging.error(" " + datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                          " A kezdő dátum nem lehet nagyobb mint a vég dátum")
            exit(1)
    else:

        startDate = yesterday.strftime("%Y.%m.%d")
        endDate = startDate

    # QUERYS:
    # get last id for aru (query script, boltok, fetchType)
    lastIdFb = int(
        queryFb("SELECT FIRST 1 ID FROM CIK ORDER BY ID DESC", boltok, "one"))

    lastIdMysql = int(queryMysql("SELECT max(id) FROM cikk", boltok, "one"))

    if lastIdFb != lastIdMysql:
        ressult = queryFb("SELECT CIK.ID, CIK.NEV, MNY.KOD, AFA.NEV FROM CIK JOIN CIKADT ON CIK_ID=CIK.ID JOIN BSR ON CIKADT.BSR_ID=BSR.ID JOIN AFA ON BSR.AFA_ID=AFA.ID JOIN CIKMNY ON CIKMNY.CIK_ID=CIK.ID JOIN MNY ON MNY.ID=CIKMNY.MNY_ID WHERE CIK.ID BETWEEN 3452735 AND 3452741", boltok, "all")
        for row in ressult:
            print(row[0])
        # get range of aru
        #getAru(lastIdFb, lastIdMysql)

    # TODO
    ### getLastIDs - done
    ### getAru (mysqlLastItem, fbLastItem)
    # aruToMysql(mysqlHost, mysqlData, mysqlUser, mysqlPass) csv -> to  mysql
    ##getForgalom (startDate, endDate)
    ### forgalomToMysql ()


if __name__ == "__main__":
    main()
