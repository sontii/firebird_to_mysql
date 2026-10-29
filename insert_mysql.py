from errormail import *
from datetime import datetime
import logging
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(filename="log/logfile.log", encoding="utf-8", level=logging.INFO)

mysqlHost = os.getenv("MYSQLHOST")
mysqlData = os.getenv("MYSQLDATA")
mysqlUser = os.getenv("MYSQLUSER")
mysqlPass = os.getenv("MYSQLPASS")


## passing parameters table name, column list ("a, b, c"), rows from Firebird
## INSERT IGNORE: duplicate keys are skipped, same as the former LOAD DATA LOCAL
def insertMysql(tableName, columns, rows):
    if not rows:
        return ()

    try:
        connection = mysql.connector.connect(
            host=mysqlHost,
            database=mysqlData,
            user=mysqlUser,
            password=mysqlPass,
            auth_plugin="mysql_native_password"
        )

        cursor = connection.cursor()

        placeholders = ", ".join(["%s"] * len(columns.split(",")))
        query = f"INSERT IGNORE INTO {tableName} ({columns}) VALUES ({placeholders})"

        ## 1000 rows per statement
        for start in range(0, len(rows), 1000):
            cursor.executemany(query, rows[start:start + 1000])

        ## after insert need commit
        connection.commit()
        cursor.close()
        connection.close()

        return ()

    except Exception as err:
        logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S")
                        + " Error while inserting to MySQL " + f"{tableName} hiba: {err}")
        errorMail(err)
        exit(1)
