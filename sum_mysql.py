from errormail import *
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import logging
import mysql.connector
from dotenv import load_dotenv

from query_fb import queryFb

load_dotenv()


logging.basicConfig(filename="log/logfile.log", encoding="utf-8", level=logging.INFO)

mysqlHost = os.getenv("MYSQLHOST")
mysqlData = os.getenv("MYSQLDATA")
mysqlUser = os.getenv("MYSQLUSER")
mysqlPass = os.getenv("MYSQLPASS")

## grill items left out of the grill totals (NP columns still include them)
GRILL_EXCLUDE = {
    8000462000, 8000457000, 8000456000, 8000455000, 8000454000,
    8000453000, 8000452000, 8000389000, 7001546000, 7001540000,
    6071355000, 4806737000, 4806736000, 4806735000, 4806720000,
    4806719000, 4806717000, 4806716000, 4806715000, 4806714000,
    4806713000, 4806712000, 4806711000, 4806710000, 4806709000,
    4806707000, 4806706000, 4806705000, 4806704000, 4806703000,
    4806702000, 4806701000, 4806700000, 4806699000, 4806697000,
    4806696000, 4806695000, 4806694000, 4806693000, 6906442816,
}


### round like MySQL ROUND on DECIMAL (half away from zero)
def rnd(value, places=0):
    if value is None:
        return None
    return Decimal(value).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


### daily store totals of receipt lines, same columns as blokk_sum / trafik_blokk_sum
def getBlokkSum(startDate, endDate, trafik):
    rows = queryFb("all", """ SELECT DATUM, EGYSEG, SUM(BTENY_ERT), SUM(NTENY_ERT), SUM(NYILV_ERT), COUNT(DISTINCT SORSZAM)
                              FROM BLOKK_TET
                              WHERE DATUM BETWEEN '%s' AND '%s'
                              GROUP BY DATUM, EGYSEG """ % (startDate, endDate), trafik)
    result = []
    for datum, egyseg, bteny, nteny, nyilv, vevoszam in rows:
        arres = nteny - nyilv
        arresSzazalek = rnd(arres * 100 / nteny, 2) if nteny else None
        result.append((datum, egyseg.strip(), rnd(bteny), rnd(arres), arresSzazalek, vevoszam, rnd(bteny - arres)))
    return result


### daily grill totals of NP-* and K-* items
def getGrill(startDate, endDate):
    rows = queryFb("all", """ SELECT BLOKK_TET.DATUM, BLOKK_TET.EGYSEG, BLOKK_TET.ARUKOD, CIK.NEV,
                                     SUM(BLOKK_TET.MENNY), SUM(BLOKK_TET.NTENY_ERT), SUM(BLOKK_TET.BTENY_ERT)
                              FROM BLOKK_TET
                              JOIN CIK ON CIK.KOD = BLOKK_TET.ARUKOD
                              WHERE BLOKK_TET.DATUM BETWEEN '%s' AND '%s'
                                AND (UPPER(CIK.NEV) LIKE 'NP-%%' OR UPPER(CIK.NEV) LIKE 'K-%%')
                              GROUP BY BLOKK_TET.DATUM, BLOKK_TET.EGYSEG, BLOKK_TET.ARUKOD, CIK.NEV """ % (startDate, endDate), False)

    ## [menny, netto, brutto, np menny, np netto, np brutto, counted items, np items]
    days = {}
    for datum, egyseg, arukod, nev, menny, nteny, bteny in rows:
        day = days.setdefault((datum, egyseg.strip()), [Decimal(0)] * 6 + [0, 0])
        if int(arukod) not in GRILL_EXCLUDE:
            day[0] += menny
            day[1] += nteny
            day[2] += bteny
            day[6] += 1
        if nev.upper().startswith("NP-"):
            day[3] += menny
            day[4] += nteny
            day[5] += bteny
            day[7] += 1

    result = []
    for (datum, egyseg), day in days.items():
        ## day without counted grill item is not a grill day
        if day[6] == 0:
            continue
        np = [rnd(x) for x in day[3:6]] if day[7] else [None, None, None]
        result.append((datum, egyseg, rnd(day[0]), rnd(day[1]), rnd(day[2]), *np))
    return result


### recompute blokk_sum, trafik_blokk_sum and grill for the date range in one transaction
def refreshSums(startDate, endDate):
    blokkSum = getBlokkSum(startDate, endDate, False)
    trafikSum = getBlokkSum(startDate, endDate, True)
    grill = getGrill(startDate, endDate)

    dateFrom = datetime.strptime(startDate, "%Y.%m.%d").date()
    dateTo = datetime.strptime(endDate, "%Y.%m.%d").date()

    try:
        connection = mysql.connector.connect(
            host=mysqlHost,
            database=mysqlData,
            user=mysqlUser,
            password=mysqlPass,
            auth_plugin="mysql_native_password"
        )
        cursor = connection.cursor()

        for table, rows in (("blokk_sum", blokkSum), ("trafik_blokk_sum", trafikSum)):
            cursor.execute(f"DELETE FROM {table} WHERE datum BETWEEN %s AND %s", (dateFrom, dateTo))
            if rows:
                cursor.executemany(f"""INSERT INTO {table} (datum, egyseg, fogyar, arres, arresszazalek, vevoszam, nettofogyar)
                                       VALUES (%s, %s, %s, %s, %s, %s, %s)""", rows)

        cursor.execute("DELETE FROM grill WHERE datum BETWEEN %s AND %s", (dateFrom, dateTo))
        if grill:
            cursor.executemany("""INSERT INTO grill (datum, egyseg, mennyiseg, ert_netto, ert_brutto,
                                                     mennyiseg_np, ert_netto_np, ert_brutto_np)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", grill)

        ## handover value comes from grill_besz, loaded earlier in the same run
        cursor.execute("""UPDATE grill SET atadas_netto =
                              (SELECT ROUND(SUM(grill_besz.ertek), 0) FROM grill_besz
                               WHERE grill_besz.datum = grill.datum AND grill_besz.egyseg = grill.egyseg)
                          WHERE datum BETWEEN %s AND %s""", (dateFrom, dateTo))

        ## after insert need commit
        connection.commit()
        cursor.close()
        connection.close()

        return ()

    except Exception as err:
        logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S")
                        + " Error while refreshing summaries" + f" hiba: {err}")
        errorMail(err)
        exit(1)
