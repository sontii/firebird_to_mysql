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
    lastIdFb = int(queryFb( boltok, "one", """ SELECT FIRST 1 ID FROM CIK ORDER BY ID DESC """))

    ## last CIKMNY id for ean
    lastCIKMNYFb = int(queryFb( boltok, "one", """ SELECT FIRST 1 ID FROM CIKMNY ORDER BY ID DESC """))

    ## last id for tiltott cikk
    lastTiltasFb = int(queryFb( boltok, "one", """ SELECT FIRST 1 ID FROM CIKUZF ORDER BY ID DESC """))

    ## last id in forgalom query by date!!
    lastForgalomFb = int(queryFb( boltok, "one", """ SELECT MAX(TRX_ID)
                                               FROM BLOKK_TET
                                               WHERE DATUM BETWEEN '%s' AND '%s' """ % (fiveDayBefore.strftime("%Y.%m.%d"), date.today().strftime("%Y.%m.%d"))))
    
    ## last stored aru id in mysql for aru
    lastIdMysql = int(queryMysql( boltok, "one", """ SELECT max(arukod) FROM cikk"""))

    ## last stored CIKMNY id in mysql for ean
    lastCIKMNYMysql = int(queryMysql( boltok, "one", """ SELECT max(cikmny_id) FROM ean"""))
   
    ## last stored id in mysql for tiltott cikk
    lastTiltasMysql = int(queryMysql( boltok, "one", """ SELECT max(tiltas_id) FROM tiltas"""))

    ## last stored id in mysql for forgalom
    lastForgalomMysql = int(queryMysql( boltok, "one", """ SELECT max(id) FROM blokk"""))

    # INSERTS:
    ## get aru from firebird and pass to mysql
    if lastIdFb != lastIdMysql:
        getQuery = """ SELECT CIK.ID, CIK.NEV, MNY.KOD, BSR.AFA_ID
                        FROM CIK
                        JOIN CIKADT ON CIK_ID=CIK.ID
                        JOIN BSR ON CIKADT.BSR_ID=BSR.ID
                        JOIN CIKMNY ON CIKMNY.CIK_ID=CIK.ID
                        JOIN MNY ON MNY.ID=CIKMNY.MNY_ID
                        WHERE CIK.ID BETWEEN %s AND %s AND KPC_ID = 1 """ % ( lastIdMysql, lastIdFb)

        aruToCsv = queryFb(boltok, "all", getQuery)
        writeToCsv(aruToCsv, "csv/aru.csv")

        ## passing parameters number, csv file path, boltok, fetch all or one, query string. query string other half is in insert_mysql.py
        insertMysql(4, "csv/aru.csv", boltok, "all", """INSERT INTO cikk (arukod, rovid_nev, me, afa_kod) VALUES (""" )

        clearCsv("csv/aru.csv")
    
    
    ## get ean and taltos from firebird and pass to mysql
    if lastCIKMNYFb != lastCIKMNYMysql:
        
        ### EAN
        getQuery = """ SELECT CIK_ID, CIKMNY.ID, CIKKOD.KOD
                       FROM CIK
                       JOIN CIKMNY ON CIKMNY.CIK_ID = CIK."ID"  
                       JOIN CIKKOD ON CIKKOD.CIKMNY_ID = CIKMNY.ID
                       LEFT JOIN CIKKODKPC ON CIKKOD.KPC_ID  = CIKKODKPC.ID
                       WHERE CIKMNY.ID BETWEEN %s AND %s AND CIKKOD.KPC_ID = 3""" % ( lastCIKMNYMysql, lastCIKMNYFb)

        ##get result from sql
        eanToCsv = queryFb(boltok, "all", getQuery)

        if eanToCsv:
            ## write result to csv
            writeToCsv(eanToCsv, "csv/ean.csv")
            
            ## passing parameters number, csv file path, boltok, fetch all or one, query string
            insertMysql( 3, "csv/ean.csv", boltok, "all", """INSERT INTO ean (arukod_id, cikmny_id, ean_kod) VALUES (""" )
            
            clearCsv("csv/ean.csv")

        ### TALTOS
        getQuery = """ SELECT CIK_ID, CIKKOD.KOD
                       FROM CIK
                       JOIN CIKMNY ON CIKMNY.CIK_ID = CIK."ID"
                       JOIN CIKKOD ON CIKKOD.CIKMNY_ID = CIKMNY.ID
                       LEFT JOIN CIKKODKPC ON CIKKOD.KPC_ID = CIKKODKPC.ID
                       WHERE CIKMNY.ID BETWEEN %s AND %s AND CIKKOD.KPC_ID = 10""" % ( lastCIKMNYMysql, lastCIKMNYFb)

        ##get result from sql
        taltosToCsv = queryFb(boltok, "all", getQuery)
        ## write result to csv
        if taltosToCsv:
            writeToCsv(taltosToCsv, "csv/taltos.csv")
        
            ## passing parameters number, csv file path, boltok, fetch all or one, query string
            insertMysql( 2, "csv/taltos.csv", boltok, "all", """INSERT INTO taltos (arukod_id, taltos_kod) VALUES (""" )
            
            clearCsv("csv/taltos.csv")
    
    if lastTiltasFb != lastTiltasMysql:
        ### tiltott cikkek
        getQuery = """ SELECT CIKUZF.ID, CIKUZF.CIK_ID, CIKUZF.UZF_ID, UZF.NEV
                       FROM CIKUZF
                       JOIN UZF ON CIKUZF.UZF_ID = UZF.ID
                       WHERE CIKUZF.ID BETWEEN %s AND %s """ % ( lastTiltasMysql, lastTiltasFb)
        ##get result from sql
        forgalomToCsv = queryFb(boltok, "all", getQuery)
        if forgalomToCsv:
        
            ## write result to csv
            writeToCsv(forgalomToCsv, "csv/tiltas.csv")

            ## passing parameters number, csv file path, boltok, fetch all or one, query string
            insertMysql( 4, "csv/tiltas.csv", boltok, "all", """INSERT INTO tiltas (tiltas_id, arukod_id, tiltas_nev_id, tiltas_nev) VALUES (""" )

            clearCsv("csv/tiltas.csv")
 
    # TODO
    if lastForgalomFb != lastForgalomMysql:
        ### blokk tetel forgalom
        getQuery = """ SELECT TRX_ID, EGYSEG, PT_GEP, DATUM, SORSZAM, ARUKOD, MENNY, ME, AFA_KOD, AFA_SZAZ, NYILV_AR, NYILV_ERT,
                        BFOGY_AR, BFOGY_ERT, NFOGY_ERT, BTENY_AR, BTENY_ERT, NTENY_ERT, NENG_ERT, BENG_ERT, TVR_AZON
                       FROM BLOKK_TET
                       WHERE DATUM BETWEEN '%s' AND '%s' AND TRX_ID BETWEEN %s AND %s """ % ( fiveDayBefore.strftime("%Y.%m.%d"), date.today().strftime("%Y.%m.%d"), lastForgalomMysql, lastForgalomFb)
        ##get result from sql
        forgalomToCsv = queryFb(boltok, "all", getQuery)
        if forgalomToCsv:
        
            ## write result to csv
            writeToCsv(forgalomToCsv, "csv/forgalom.csv")

            ## passing parameters number, csv file path, boltok, fetch all or one, query string
            insertMysql( 14, "csv/forgalom.csv", boltok, "all", """INSERT INTO blokk
                                     (id, egyseg, pt_gep, datum, sorszam, arukod_id, menny, me, afa_kod. afa_szaz, nyilv_ar, nyilv_ertek,
                                     bfogy_ar, bfogy_ert, nfogy_ert, bteny_ar, bteny_ert, nteny_ert, neng_ert, beng_ert, tvr_azon) VALUES (""" )

            clearCsv("csv/forgalom.csv")

if __name__ == "__main__":
    main()
