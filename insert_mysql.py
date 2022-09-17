from errormail import *
from datetime import datetime
import logging
import mysql.connector
import csv
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)

mysqlHost = os.getenv("MYSQLHOST")
mysqlData = os.getenv("MYSQLDATA")
mysqlUser = os.getenv("MYSQLUSER")
mysqlPass = os.getenv("MYSQLPASS")


def insertMysql(readFile, boltok, fetchType, query):
    try:
        connection = mysql.connector.connect(host=mysqlHost,
                                             database=mysqlData,
                                             user=mysqlUser,
                                             password=mysqlPass,
                                             auth_plugin='mysql_native_password')

        cursor = connection.cursor()

        with open(readFile, encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader)
            insert_sql = query + "VALUES( %s, %s, %s, %s)"

            rows = []
            row_count = 0
            for row in csv_reader:
                row_count += 1
                rows.append(row)
                if row_count == 1000:
                    cursor.executemany(insert_sql, rows)
                    rows = []
                    row_count = 0
            if rows:
                cursor.executemany(insert_sql, rows)

        connection.commit()
        cursor.close()
        connection.close()

        return ()

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                      " Error while connecting to MySQL " + f"{err}")
        errorMail(err)
        exit(1)
