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
from insert_mysql import *
from sum_mysql import *

load_dotenv()

logging.basicConfig(filename="log/logfile.log", encoding="utf-8", level=logging.INFO)

## create dict from list
envBoltok = os.getenv('BOLTOK').split(",")
boltok = {}
for bolt in envBoltok:
    boltok[bolt] = None

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
    ## summaries of the last week are recomputed every run (late rows)
    weekBefore = date.today() - timedelta(days=7)

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
        startDate = weekBefore.strftime("%Y.%m.%d")
        endDate = date.today().strftime("%Y.%m.%d")

    # QUERYS:
    # FIREBIRD
    ## get last id for aru (fetchType, query script, isTrafik)
    lastAruFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CIK ORDER BY ID DESC """, False)[0])

    ## last CIKMNY id for ean
    lastCIKMNYFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CIKMNY ORDER BY ID DESC """, False)[0])

    ## last id for tiltott cikk
    lastTiltasFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CIKUZF ORDER BY ID DESC """, False)[0])

    ## last id for Nomenklatura
    lastNomenklaturaFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CCS WHERE HIERARCHIA_SZINT = 5 ORDER BY ID DESC  """, False)[0])

    ## last id for cikk_csoport
    lastCikk_csoportFb = int(queryFb("one", """ SELECT FIRST 1 ID FROM CCSCIK ORDER BY ID DESC """, False)[0])
  
  


    # MYSQL
    ## last stored aru id in mysql for aru
    lastAruMysql = int(queryMysql("one", """ SELECT max(cikk_id) FROM cikk"""))

    ## last stored CIKMNY id in mysql for ean
    lastCIKMNYMysql = int(queryMysql("one", """ SELECT max(cikmny_id) FROM ean"""))

    ## last stored id in mysql for tiltott cikk
    lastTiltasMysql = int(queryMysql("one", """ SELECT max(tiltas_id) FROM tiltas"""))

    ## last stored id in mysql for nomenklatura
    lastNomenklaturaMysql = int(queryMysql("one", """ SELECT max(id) FROM nomenklatura"""))

    ## last stored id in mysql for Cikk_csoport
    lastCikk_csoportMysql = int(queryMysql("one", """ SELECT max(id) FROM cikk_csoport"""))

    ## last stored date in mysql for Grill_besz
    lastGrillBznMysql = queryMysql("one", """ SELECT max(datum) FROM grill_besz""")

    ## last stored date in mysql for logisztika
    lastLogisztikaMysql = queryMysql("one", """ SELECT max(datum) FROM logisztika""")



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

        aruRows = queryFb( "all", getQuery, False)

        ## insert to Mysql: table, columns, rows
        insertMysql('cikk', 'cikk_id, cikk_kod, rovid_nev, me, afa_id', aruRows)

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
        eanRows = queryFb("all", getQuery, False)

        if eanRows:
            ## insert to Mysql: table, columns, rows
            insertMysql('ean', 'arukod_id, cikmny_id, ean_kod', eanRows)

        ### TALTOS
        getQuery = """ SELECT CIK_ID, CIKKOD.KOD
                       FROM CIK
                       JOIN CIKMNY ON CIKMNY.CIK_ID = CIK."ID"
                       JOIN CIKKOD ON CIKKOD.CIKMNY_ID = CIKMNY.ID
                       LEFT JOIN CIKKODKPC ON CIKKOD.KPC_ID = CIKKODKPC.ID
                       WHERE CIKMNY.ID BETWEEN %s AND %s AND CIKKOD.KPC_ID = 10""" % ( lastCIKMNYMysql + 1, lastCIKMNYFb)

        ##get result from sql
        taltosRows = queryFb("all", getQuery, False)
        if taltosRows:
            ## insert to Mysql: table, columns, rows
            insertMysql('taltos', 'arukod_id, taltos_kod', taltosRows)

    if lastTiltasFb != lastTiltasMysql:
        ### tiltott cikkek
        getQuery = """ SELECT CIKUZF.ID, CIKUZF.CIK_ID, CIKUZF.UZF_ID, UZF.NEV
                       FROM CIKUZF
                       JOIN UZF ON CIKUZF.UZF_ID = UZF.ID
                       WHERE CIKUZF.ID BETWEEN %s AND %s """ % ( lastTiltasMysql + 1, lastTiltasFb)
        ##get result from sql
        tiltasRows = queryFb("all", getQuery, False)
        if tiltasRows:

            ## insert to Mysql: table, columns, rows
            insertMysql('tiltas', 'tiltas_id, arukod_id, tiltas_nev_id, tiltas_nev', tiltasRows)

    if lastNomenklaturaFb != lastNomenklaturaMysql:
        ### nomenklatura besorolas
        getQuery = """ SELECT CCS.ID, CCS.KOD, CCS.NEV, CCS.HIERARCHIA_SZINT
                       FROM CCS
                       WHERE CCS.ID BETWEEN %s AND %s  """ % ( lastNomenklaturaMysql + 1, lastNomenklaturaFb)
        
        ##get result from sql
        nomenklaturaRows = queryFb("all", getQuery, False)
        if nomenklaturaRows:

            ## insert to Mysql: table, columns, rows
            insertMysql('nomenklatura', 'id, kod, nev, szint', nomenklaturaRows)
    
    if lastCikk_csoportFb != lastCikk_csoportMysql:
        ### nomenklatura besorolas
        getQuery = """ SELECT CCSCIK.ID, CCSCIK.CCS_ID, CCSCIK.CIK_ID 
                       FROM CCSCIK
                       WHERE CCSCIK.ID BETWEEN %s AND %s """ % ( lastCikk_csoportMysql + 1, lastCikk_csoportFb)
        
        ##get result from sql
        cikk_csoportRows = queryFb("all", getQuery, False)
        if cikk_csoportRows:

            ## insert to Mysql: table, columns, rows
            insertMysql('cikk_csoport', 'id, nomen_id, arukod_id', cikk_csoportRows)
    

    if lastGrillBznMysql < yesterday:

        getQuery = """ SELECT
                            CAST(BZN.BIZONYLAT_DATUM AS DATE) AS datum,
                            BZN.EGYSEG_KOD,
                            SUM(BZNTETART.ERTEK),
                            BZNTET.MOZGAS_MINOSITO_KOD
                        FROM BZNTET
                        JOIN BZNTETART ON BZNTET.ID = BZNTETART.BZNTET_ID
                        JOIN BZN ON BZNTET.BZN_ID = BZN.ID
                        WHERE BZN.BIZONYLAT_DATUM BETWEEN '%s' AND '%s'
                        GROUP BY BZNTETART.BZNARTTPS_ID, BZNTET.MOZGAS_MINOSITO_KOD, BZN.BIZONYLAT_DATUM, BZN.EGYSEG_KOD 
                        HAVING (BZNTETART.BZNARTTPS_ID = 17 AND (BZNTET.MOZGAS_MINOSITO_KOD= '103' OR BZNTET.MOZGAS_MINOSITO_KOD= '114'))
                        ORDER BY CAST(BZN.BIZONYLAT_DATUM AS DATE)""" %(lastGrillBznMysql + timedelta(days=1), yesterday)

        ##get result from sql
        grillBeszRows = queryFb("fetchAll", getQuery, False)
        
        if grillBeszRows:

            ## insert to Mysql: table, columns, rows
            insertMysql('grill_besz', 'datum, egyseg, ertek, minosito_kod', grillBeszRows)
    

    if lastLogisztikaMysql < yesterday:

        getQuery = """ SELECT
                            CAST(BZN.BIZONYLAT_DATUM AS DATE) AS datum,
                            Sum(CASE WHEN BZNARTTPS.ID = (19) AND BZNTPSHIS.KOD = 'BEVGYAR' THEN BZNTETART.ERTEK END) AS forg_brutto,
                            Sum(CASE WHEN BZNARTTPS.ID = (18) AND BZNTPSHIS.KOD = 'BEVGYAR' THEN BZNTETART.ERTEK END) AS forg_netto,
                            Sum(CASE WHEN BZNARTTPS.ID = (17) AND BZNTPSHIS.KOD = 'KIADGYAR' THEN BZNTETART.ERTEK END) AS forg_netto_nyilv,
                            Sum(CASE WHEN BZNARTTPS.ID = (18) AND BZNTPSHIS.KOD = 'BEVGYAR' THEN BZNTET.MENNYISEG END) AS vevo	
                        FROM BZN
                        JOIN BZNTET ON BZN.ID = BZNTET.BZN_ID
                        JOIN BZNTETART ON BZNTET.ID = BZNTETART.BZNTET_ID
                        JOIN BZNARTTPS ON BZNTETART.BZNARTTPS_ID = BZNARTTPS.ID
                        JOIN BZNTPSHIS ON BZNTPSHIS.ID = BZN.BZNTPSHIS_ID
                        WHERE BZN.BIZONYLAT_DATUM BETWEEN '%s' AND '%s 23:59:59'
                        GROUP BY CAST(BZN.BIZONYLAT_DATUM AS DATE) """ %(lastLogisztikaMysql + timedelta(days=1),yesterday)

        ##get result from sql
        logisztikaRows = queryFb("fetchAll", getQuery, False)
        
        if logisztikaRows:

            ## insert to Mysql: table, columns, rows
            insertMysql('logisztika', 'datum, brutto_fogy, netto_fogy, netto_nyilv, vevo', logisztikaRows)


    ## daily summaries (blokk_sum, trafik_blokk_sum, grill) straight from Laurel
    ## after grill_besz, the grill handover value is read from it
    refreshSums(startDate, endDate)

    end_time = datetime.now()
    logging.info('start query' + f" Duration: {end_time - start_time}")

    ## keep logs short. if need:
    ## end_time = datetime.now()
    ## logging.info(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S") + f" Duration: {end_time - start_time}")


if __name__ == "__main__":
    main()
