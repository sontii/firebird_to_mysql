from errormail import *
from datetime import datetime
import logging
import mysql.connector
import csv
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(filename="log/logfile.log", encoding="utf-8", level=logging.INFO)

mysqlHost = os.getenv("MYSQLHOST")
mysqlData = os.getenv("MYSQLDATA")
mysqlUser = os.getenv("MYSQLUSER")
mysqlPass = os.getenv("MYSQLPASS")


## passing parameters number, csv file path, boltok, fetch all or one, query string
def insertMysql(valuesNr, readFile, boltok, fetchType, query):
    try:
        connection = mysql.connector.connect(
            host=mysqlHost,
            database=mysqlData,
            user=mysqlUser,
            password=mysqlPass,
            auth_plugin="mysql_native_password",
        )

        cursor = connection.cursor()

        ## open csv file
        with open(readFile, encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            next(csv_reader)

            ## cannot pass string %s params, build up here VALUES part
            for value in range(0, valuesNr):
                query = query + " %s,"
            ## remove tail ","
            query = query[:-1]
            query = query + ")"

            ## read 1000 row then execute many, and repeat
            rows = []
            row_count = 0
            for row in csv_reader:
                row_count += 1
                rows.append(row)
                if row_count == 1000:
                    cursor.executemany(query, rows)
                    rows = []
                    row_count = 0
            if rows:
                cursor.executemany(query, rows)

        ## after insert need commit
        connection.commit()
        cursor.close()
        connection.close()

        return ()

    except Exception as err:
        logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S") 
                        + " Error while connecting to MySQL" + f" {query}" + f" {err}")
        errorMail(err)
        exit(1)
