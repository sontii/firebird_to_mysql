getQuery = """ SELECT TRX_ID, EGYSEG, PT_GEP, DATUM, SORSZAM, ARUKOD, MENNY, ME, AFA_KOD, AFA_SZAZ, NYILV_AR, NYILV_ERT,
                        BFOGY_AR, BFOGY_ERT, NFOGY_ERT, BTENY_AR, BTENY_ERT, NTENY_ERT, NENG_ERT, BENG_ERT, TVR_AZON
                       FROM BLOKK_TET
                       WHERE DATUM BETWEEN '2021.01.01' AND '2021.01.10' AND TRX_ID BETWEEN %s AND %s """ % ( fiveDayBefore.strftime("%Y.%m.%d"), date.today().strftime("%Y.%m.%d"), lastForgalomMysql, lastForgalomMaxFb)