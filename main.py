import sys

## cron job set relativ path
import os
scriptdir =  os.path.dirname(os.path.abspath(__file__))
os.chdir(scriptdir)

import datetime
from datetime import date, datetime, timedelta
import logging
from dotenv import load_dotenv

from query_fb import *
from query_mysql import *
from insert_mysql_bulk import *
from writetocsv import *

### from insert_mysql import *
### ## passing parameters number, csv file path, fetch all or one, query string
### insertMysql( 3, "ean.csv", "all", """INSERT INTO ean (arukod_id, cikmny_id, ean_kod) VALUES (""" )

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


### validate date format befor using it
def validateDate(date_text):
    try:
        datetime.strptime(date_text, "%Y.%m.%d")
    except ValueError:
        logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S")
                          + " not valid date format yyyy.mm.dd")
        exit('not valid date format yyyy.mm.dd')

def main():
    ## log duration enable at bottom
    start_time = datetime.now()

    yesterday = date.today() - timedelta(days=1)
    fiveDayBefore = date.today() - timedelta(days=5)

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
        startDate = fiveDayBefore.strftime("%Y.%m.%d")
        endDate = date.today().strftime("%Y.%m.%d")

    # QUERYS:
    # FIREBIRD
    ## get last id for aru (fetchType, query script)
    lastAruFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CIK ORDER BY ID DESC """)[0])

    ## last CIKMNY id for ean
    lastCIKMNYFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CIKMNY ORDER BY ID DESC """)[0])

    ## last id for tiltott cikk
    lastTiltasFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CIKUZF ORDER BY ID DESC """)[0])

    ## last MIN-MAX id in forgalom query by date!!
    lastForgalomFb = queryFb("one", """ SELECT MIN(TRX_ID), MAX(TRX_ID)
                                               FROM BLOKK_TET
                                               WHERE DATUM BETWEEN '%s' AND '%s' """ % (startDate, endDate))
    
    ## last id for Nomenklatura
    lastNomenklaturaFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CCS WHERE HIERARCHIA_SZINT = 5 ORDER BY ID DESC  """)[0])

    ## last id for cikk_csoport
    lastCikk_csoportFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CCSCIK ORDER BY ID DESC """)[0])

    ## last id for grillbesz
    ## lastGrillBznFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM BZN ORDER BY ID DESC """)[0])

    # MYSQL
    ## last stored aru id in mysql for aru
    lastAruMysql = int(queryMysql("one", """ SELECT max(cikk_id) FROM cikk"""))

    ## last stored CIKMNY id in mysql for ean
    lastCIKMNYMysql = int(queryMysql("one", """ SELECT max(cikmny_id) FROM ean"""))

    ## last stored id in mysql for tiltott cikk
    lastTiltasMysql = int(queryMysql("one", """ SELECT max(tiltas_id) FROM tiltas"""))

    ## last stored id in mysql for forgalom
    lastForgalomMysql = int(queryMysql("one", """ SELECT max(id) FROM blokk"""))

    ## last stored id in mysql for nomenklatura
    lastNomenklaturaMysql = int(queryMysql("one", """ SELECT max(id) FROM nomenklatura"""))

    ## last stored id in mysql for Cikk_csoport
    lastCikk_csoportMysql = int(queryMysql("one", """ SELECT max(id) FROM cikk_csoport"""))

    ## last stored id in mysql for Cikk_csoport
    ## lastGrillBznMysql = int(queryMysql("one", """ SELECT max(id) FROM bzn_grill"""))

    """ SELECT  BZN.BIZONYLAT_DATUM, BZN.EGYSEG_KOD, SUM(bzntetart.ertek), bzntet.mozgas_minosito_kod
        FROM bzntet 
        JOIN bzntetart on bzntet.id = bzntetart.bzntet_id
        JOIN BZN ON BZNTET.BZN_ID = BZN.ID
        WHERE BZN.BIZONYLAT_DATUM BETWEEN '2022.06.23' AND '2023.06.23 23:59:59'
        GROUP BY bzntet.bzn_id, bzntetart.bznarttps_id, bzntet.mozgas_minosito_kod, BZN.BIZONYLAT_DATUM, BZN.EGYSEG_KOD 
        HAVING (bzntetart.bznarttps_id = 17 and (bzntet.mozgas_minosito_kod= '103' or bzntet.mozgas_minosito_kod= '114')) """

    getQuery = """ SELECT BZN.BIZONYLAT_DATUM, BZN.EGYSEG_KOD, SUM(BZNTETART.ERTEK), BZNTET.MOZGAS_MINOSITO_KOD 
                    FROM BZNTET
                    JOIN BZNTETART ON BZNTET.ID = BZNTETART.BZNTET_ID
                    JOIN BZN ON BZNTET.BZN_ID = BZN.ID
                    WHERE BZN.BIZONYLAT_DATUM BETWEEN '2022.05.01' AND '2023.06.15 23:59:59'
                    GROUP BY BZNTET.BZN_ID, BZNTETART.BZNARTTPS_ID, BZNTET.MOZGAS_MINOSITO_KOD, BZN.BIZONYLAT_DATUM, BZN.EGYSEG_KOD 
                    HAVING (BZNTETART.BZNARTTPS_ID = 17 AND (BZNTET.MOZGAS_MINOSITO_KOD= '103' OR BZNTET.MOZGAS_MINOSITO_KOD= '114'))
                    ORDER BY BZN.BIZONYLAT_DATUM """

    ##get result from sql
    grillBeszToCsv = queryFb("fetchAll", getQuery)
    
    if grillBeszToCsv:

        for index, data in enumerate(grillBeszToCsv):
            row = list(data)
            row[0] = row[0].date()
            row.insert(0, '')
            data = tuple(row)

            grillBeszToCsv[index] = data

        ## write result to csv
        writeToCsv(grillBeszToCsv, "csv/grill_besz.csv")

        
        ## Bulk insert to Mysql, csv - table name
        insertMysqlBulk('csv/grill_besz.csv', 'grill_besz')

        clearCsv("csv/grill_besz.csv")

    exit(1)

    # INSERTS:
    ## get aru from firebird and pass to mysql

    if lastAruFb != lastAruMysql:
        getQuery = """ SELECT CIK.ID, CIK.KOD, CIK.NEV, MNY.KOD, BSR.AFA_ID
                        FROM CIK
                        JOIN CIKADT ON CIK_ID=CIK.ID
                        JOIN BSR ON CIKADT.BSR_ID=BSR.ID
                        JOIN CIKMNY ON CIKMNY.CIK_ID=CIK.ID
                        JOIN MNY ON MNY.ID=CIKMNY.MNY_ID
                        WHERE CIK.ID BETWEEN %s AND %s AND KPC_ID = 1 """ % ( lastAruMysql + 1, lastAruFb)

        aruToCsv = queryFb( "all", getQuery)

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
        eanToCsv = queryFb("all", getQuery)

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
        taltosToCsv = queryFb("all", getQuery)
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
        tiltasToCsv = queryFb("all", getQuery)
        if tiltasToCsv:

            ## write result to csv
            writeToCsv(tiltasToCsv, "csv/tiltas.csv")

            ## Bulk insert to Mysql, csv - table name
            insertMysqlBulk('csv/tiltas.csv', 'tiltas')

            clearCsv("csv/tiltas.csv")

    if lastNomenklaturaFb != lastNomenklaturaMysql:
        ### nomenklatura besorolas
        getQuery = """ SELECT CCS.ID, CCS.KOD, CCS.NEV, CCS.HIERARCHIA_SZINT
                       FROM CCS
                       WHERE CCS.ID BETWEEN %s AND %s  """ % ( lastNomenklaturaMysql + 1, lastNomenklaturaFb)
        
        ##get result from sql
        nomenklaturaToCsv = queryFb("all", getQuery)
        if nomenklaturaToCsv:

            ## write result to csv
            writeToCsv(nomenklaturaToCsv, "csv/nomenklatura.csv")

            ## Bulk insert to Mysql, csv - table name
            insertMysqlBulk('csv/nomenklatura.csv', 'nomenklatura')

            clearCsv("csv/nomenklatura.csv")
    
    if lastCikk_csoportFb != lastCikk_csoportMysql:
        ### nomenklatura besorolas
        getQuery = """ SELECT CCSCIK.ID, CCSCIK.CCS_ID, CCSCIK.CIK_ID 
                       FROM CCSCIK
                       WHERE CCSCIK.ID BETWEEN %s AND %s """ % ( lastCikk_csoportMysql + 1, lastCikk_csoportFb)
        
        ##get result from sql
        cikk_csoportToCsv = queryFb("all", getQuery)
        if cikk_csoportToCsv:

            ## write result to csv
            writeToCsv(cikk_csoportToCsv, "csv/cikk_csoport.csv")

            ## Bulk insert to Mysql, csv - table name
            insertMysqlBulk('csv/cikk_csoport.csv', 'cikk_csoport')

            clearCsv("csv/cikk_csoport.csv")
    

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
                        WHERE DATUM BETWEEN '%s' AND '%s' AND TRX_ID BETWEEN %s AND %s """ % (startDate, endDate, lastForgalomMysql + 1, lastForgalomMaxFb)

            ##get result from sql
            forgalomToCsv = queryFb("all", getQuery)

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
