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
def insertMysqlBulk(readFile, tableName):
    try:
        connection = mysql.connector.connect(
            host=mysqlHost,
            database=mysqlData,
            user=mysqlUser,
            password=mysqlPass,
            auth_plugin="mysql_native_password",
            allow_local_infile=True,
            autocommit=True
        )

        load_sql = f"LOAD DATA LOCAL INFILE '{readFile}' INTO TABLE {tableName} FIELDS TERMINATED BY ',' ENCLOSED BY ''"

        cursor = connection.cursor()

        cursor.execute(load_sql)
        
        cursor.close()
        connection.close()

        return ()

    except Exception as err:
        logging.error(" " + datetime.now().strftime("%Y.%m.%d %H:%M:%S") 
                        + " Error while connecting to MySQL" + f"{readFile} hiba: {err}")
        errorMail(err)
        exit(1)
