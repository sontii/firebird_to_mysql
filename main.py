from mysql_scripts.csv_to_my_aru import *
from fb_scipts.get_lastaru_fb import *
from fb_scipts.get_forgalom import *
from fb_scipts.get_aru import *
from mysql_scripts.get_lastaru_my import *
from fb_scipts.fb_query import *
import sys
import datetime
from datetime import date, timedelta
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)

# enviroment variables setup
fbHost = os.getenv("FBHOST")
fbData = os.getenv("FBDATA")
fbUser = os.getenv("FBUSER")
fbPass = os.getenv("FBPASS")

mysqlHost = os.getenv("MYSQLHOST")
mysqlData = os.getenv("MYSQLDATA")
mysqlUser = os.getenv("MYSQLUSER")
mysqlPass = os.getenv("MYSQLPASS")

envBoltok = os.getenv("BOLTOK")

boltok = []
for bolt in envBoltok.split(","):
    boltok.append(bolt)

selectLastAruId= ("SELECT FIRST 1 ID FROM CIK ORDER BY ID DESC")

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

    lastIdFb = fbQuery(fbHost, fbData, fbUser, fbPass, selectLastAruId)
    lastIdMysql = getLastaruMysql(mysqlHost, mysqlData, mysqlUser, mysqlPass)

    getAru(lastIdFb, lastIdMysql)

# TODO
### getLastIDs - done
### getAru (mysqlLastItem, fbLastItem)
# aruToMysql(mysqlHost, mysqlData, mysqlUser, mysqlPass) csv -> to  mysql
##getForgalom (startDate, endDate)
### forgalomToMysql ()


if __name__ == "__main__":
    main()
