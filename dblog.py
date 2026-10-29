## shared log database (applog) for every project
## - every logging record goes to applog.log as well (setupDbLog)
## - the scheduler writes one applog.futas row per run (startRun / endRun)
## The file log stays as it is: if the log database is down, nothing else breaks.
import logging
import os
import socket
import sys
import threading
import traceback
import uuid

import mysql.connector

PROJEKT = os.getenv("LOG_PROJEKT", "ismeretlen")
GEP = socket.gethostname()

## logging from inside the handler (eg. by the MySQL driver) must not loop back
_inside = threading.local()


def _execute(sql, params):
    if not os.getenv("LOGDB_HOST"):
        return False
    try:
        connection = mysql.connector.connect(
            host=os.getenv("LOGDB_HOST"),
            database=os.getenv("LOGDB_NAME", "applog"),
            user=os.getenv("LOGDB_USER"),
            password=os.getenv("LOGDB_PASS"),
            connection_timeout=5,
            autocommit=True
        )
        cursor = connection.cursor()
        cursor.execute(sql, params)
        cursor.close()
        connection.close()
        return True
    except Exception as err:
        print(f"applog hiba: {err}", file=sys.stderr, flush=True)
        return False


class DbLogHandler(logging.Handler):
    def emit(self, record):
        if getattr(_inside, "active", False):
            return
        _inside.active = True
        try:
            details = None
            if record.exc_info:
                details = "".join(traceback.format_exception(*record.exc_info))
            _execute("""INSERT INTO log (projekt, futas_id, szint, uzenet, reszletek, gep)
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                     (PROJEKT, os.getenv("LOG_FUTAS_ID"), record.levelname, record.getMessage(), details, GEP))
        finally:
            _inside.active = False


### attach the log database to the root logger, call after logging.basicConfig
def setupDbLog(level=logging.INFO):
    root = logging.getLogger()
    if not any(isinstance(handler, DbLogHandler) for handler in root.handlers):
        root.addHandler(DbLogHandler(level=level))

    ## uncaught exceptions: traceback to the log database too
    def excepthook(excType, excValue, excTraceback):
        root.error(f"{excType.__name__}: {excValue}", exc_info=(excType, excValue, excTraceback))
        sys.__excepthook__(excType, excValue, excTraceback)

    sys.excepthook = excepthook


### one applog.futas row per scheduled run
def startRun():
    runId = str(uuid.uuid4())
    _execute("INSERT INTO futas (id, projekt, gep) VALUES (%s, %s, %s)", (runId, PROJEKT, GEP))
    return runId


def endRun(runId, exitCode):
    _execute("UPDATE futas SET veg = NOW(3), kilepesi_kod = %s, statusz = %s WHERE id = %s",
             (exitCode, "ok" if exitCode == 0 else "hiba", runId))
