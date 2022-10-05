import sys
import os
scriptdir =  os.path.dirname(os.path.abspath(__file__))
os.chdir(scriptdir)

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

## create dict from list
envBoltok = os.getenv('BOLTOK').split(",")
boltok = {}
for bolt in envBoltok:
    boltok[bolt] = None

vasrnapZarva = os.getenv('VASARNAPZARVA').split(",")

## copy boltok dict
sundayOpen = dict(boltok)
for bolt in sundayOpen:
    sundayOpen[bolt] = True
    if bolt in vasrnapZarva:
        sundayOpen[bolt] = False

# check argv date is valid

### clear csv file after insert
def clearCsv(fileToClear):
    # opening the file with w+ mode truncates the file
    f = open(fileToClear, "w+")
    f.close()

### check if date is sunday
def dateSunday(yesterday):
    if date.weekday(yesterday) == 6:
        return True
    return False

### check if date is sunday or holiday
def dateHoliday(yesterday):
    hu_holidays = holidays.HU()
    if yesterday in hu_holidays:
        return True
    return False


### validate date format befor using it
def validateDate(date_text):
    try:
        datetime.datetime.strptime(date_text, "%Y.%m.%d")
    except ValueError:
        logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S")
                          + " not valid date format yyyy.mm.dd")
        exit('not valid date format yyyy.mm.dd')

def main():
    ## log duration enable at bottom
    start_time = datetime.now()

    yesterday = date.today() - timedelta(days=1)
    fiveDayBefore = date.today() - timedelta(days=5)
    holiday = dateHoliday(yesterday)
    sunday = dateSunday(yesterday)

    # if argv set dates
    if len(sys.argv) == 3:
        startDate = sys.argv[1]
        endDate = sys.argv[2]
        validateDate(startDate)
        validateDate(endDate)
        if startDate > endDate:
            logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S") 
                              + " start date cannot be greater than end date")
            exit('start date cannot be greater than end date')
    else:
        startDate = yesterday.strftime("%Y.%m.%d")
        endDate = startDate


    # QUERYS:
    ## get last id for aru (boltok, fetchType, query script)
    lastAruFb = int(queryFb( boltok, "one", """ SELECT FIRST 1 ID FROM CIK ORDER BY ID DESC """)[0])

    ## last CIKMNY id for ean
    lastCIKMNYFb = int(queryFb( boltok, "one", """ SELECT FIRST 1 ID FROM CIKMNY ORDER BY ID DESC """)[0])

    ## last id for tiltott cikk
    lastTiltasFb = int(queryFb( boltok, "one", """ SELECT FIRST 1 ID FROM CIKUZF ORDER BY ID DESC """)[0])

    ## last MIN-MAX id in forgalom query by date!!
    lastForgalomFb = queryFb( boltok, "one", """ SELECT MIN(TRX_ID), MAX(TRX_ID)
                                               FROM BLOKK_TET
                                               WHERE DATUM BETWEEN '%s' AND '%s' """ % (fiveDayBefore.strftime("%Y.%m.%d"), date.today().strftime("%Y.%m.%d")))

    ## last stored aru id in mysql for aru
    lastAruMysql = int(queryMysql( boltok, "one", """ SELECT max(arukod) FROM cikk"""))

    ## last stored CIKMNY id in mysql for ean
    lastCIKMNYMysql = int(queryMysql( boltok, "one", """ SELECT max(cikmny_id) FROM ean"""))

    ## last stored id in mysql for tiltott cikk
    lastTiltasMysql = int(queryMysql( boltok, "one", """ SELECT max(tiltas_id) FROM tiltas"""))

    ## last stored id in mysql for forgalom
    lastForgalomMysql = int(queryMysql( boltok, "one", """ SELECT max(id) FROM blokk"""))

    # INSERTS:
    ## get aru from firebird and pass to mysql

    if lastAruFb != lastAruMysql:
        getQuery = """ SELECT CIK.ID, CIK.NEV, MNY.KOD, BSR.AFA_ID
                        FROM CIK
                        JOIN CIKADT ON CIK_ID=CIK.ID
                        JOIN BSR ON CIKADT.BSR_ID=BSR.ID
                        JOIN CIKMNY ON CIKMNY.CIK_ID=CIK.ID
                        JOIN MNY ON MNY.ID=CIKMNY.MNY_ID
                        WHERE CIK.ID BETWEEN %s AND %s AND KPC_ID = 1 """ % ( lastAruMysql + 1, lastAruFb)

        aruToCsv = queryFb(boltok, "all", getQuery)

        writeToCsv(aruToCsv, "csv/aru.csv")

        ## Bulk insert to Mysql, csv - table name
        insertMysqlBulk('csv/aru.csv', 'cikk')

        clearCsv("csv/aru.csv")

    ## get ean and taltos from firebird and pass to mysql
    if lastCIKMNYFb != lastCIKMNYMysql:

        ### EAN
        getQuery = """ SELECT CIK_ID, CIKMNY.ID, CIKKOD.KOD
                       FROM CIK
                       JOIN CIKMNY ON CIKMNY.CIK_ID = CIK."ID"
                       JOIN CIKKOD ON CIKKOD.CIKMNY_ID = CIKMNY.ID
                       LEFT JOIN CIKKODKPC ON CIKKOD.KPC_ID  = CIKKODKPC.ID
                       WHERE CIKMNY.ID BETWEEN %s AND %s AND CIKKOD.KPC_ID = 3""" % ( lastCIKMNYMysql + 1, lastCIKMNYFb)

        ##get result from sql
        eanToCsv = queryFb(boltok, "all", getQuery)

        if eanToCsv:
            ## write result to csv
            writeToCsv(eanToCsv, "csv/ean.csv")

            ## Bulk insert to Mysql, csv - table name
            insertMysqlBulk('csv/ean.csv', 'ean')

            clearCsv("csv/ean.csv")

        ### TALTOS
        getQuery = """ SELECT CIK_ID, CIKKOD.KOD
                       FROM CIK
                       JOIN CIKMNY ON CIKMNY.CIK_ID = CIK."ID"
                       JOIN CIKKOD ON CIKKOD.CIKMNY_ID = CIKMNY.ID
                       LEFT JOIN CIKKODKPC ON CIKKOD.KPC_ID = CIKKODKPC.ID
                       WHERE CIKMNY.ID BETWEEN %s AND %s AND CIKKOD.KPC_ID = 10""" % ( lastCIKMNYMysql + 1, lastCIKMNYFb)

        ##get result from sql
        taltosToCsv = queryFb(boltok, "all", getQuery)
        ## write result to csv
        if taltosToCsv:
            writeToCsv(taltosToCsv, "csv/taltos.csv")

            ## Bulk insert to Mysql, csv - table name
            insertMysqlBulk('csv/taltos.csv', 'taltos')

            clearCsv("csv/taltos.csv")

    if lastTiltasFb != lastTiltasMysql:
        ### tiltott cikkek
        getQuery = """ SELECT CIKUZF.ID, CIKUZF.CIK_ID, CIKUZF.UZF_ID, UZF.NEV
                       FROM CIKUZF
                       JOIN UZF ON CIKUZF.UZF_ID = UZF.ID
                       WHERE CIKUZF.ID BETWEEN %s AND %s """ % ( lastTiltasMysql + 1, lastTiltasFb)
        ##get result from sql
        forgalomToCsv = queryFb(boltok, "all", getQuery)
        if forgalomToCsv:

            ## write result to csv
            writeToCsv(forgalomToCsv, "csv/tiltas.csv")

            ## Bulk insert to Mysql, csv - table name
            insertMysqlBulk('csv/tiltas.csv', 'tiltas')

            clearCsv("csv/tiltas.csv")

    ## if have forgalom data run
    if lastForgalomFb:
        lastForgalomMinFb = int(lastForgalomFb[0])
        lastForgalomMaxFb = int(lastForgalomFb[1])

        if lastForgalomMaxFb != lastForgalomMysql:
            ###
            if lastForgalomMinFb > lastForgalomMysql:
                lastForgalomMysql = lastForgalomMinFb

            ### blokk tetel forgalom
            getQuery = """ SELECT TRX_ID, EGYSEG, PT_GEP, DATUM, SORSZAM, ARUKOD, MENNY, ME, AFA_KOD, AFA_SZAZ, NYILV_AR, NYILV_ERT,
                            BFOGY_AR, BFOGY_ERT, NFOGY_ERT, BTENY_AR, BTENY_ERT, NTENY_ERT, NENG_ERT, BENG_ERT, TVR_AZON
                        FROM BLOKK_TET
                        WHERE DATUM BETWEEN '%s' AND '%s' AND TRX_ID BETWEEN %s AND %s """ % (fiveDayBefore.strftime("%Y.%m.%d"), date.today().strftime("%Y.%m.%d"), lastForgalomMysql + 1, lastForgalomMaxFb)

            ##get result from sql
            forgalomToCsv = queryFb(boltok, "all", getQuery)

            if forgalomToCsv:

                ## write result to csv
                writeToCsv(forgalomToCsv, "csv/forgalom.csv")

                ## Bulk insert to Mysql, csv - table name
                insertMysqlBulk('csv/forgalom.csv', 'blokk')

                clearCsv("csv/forgalom.csv")


    end_time = datetime.now()
    print('start query' + f" Duration: {end_time - start_time}")

    ## keep logs short. if need:
    ## end_time = datetime.now()
    ## logging.info(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S") + f" Duration: {end_time - start_time}")


if __name__ == "__main__":
    main()
