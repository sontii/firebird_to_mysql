import csv
import datetime
import logging
import mysql.connector
from dotenv import load_dotenv
import os

from errormail import *

logging.basicConfig(filename="log/logfile.log", encoding='utf-8', level=logging.INFO)

load_dotenv()

mysqlHost = os.getenv("MYSQLHOST")
mysqlData = os.getenv("MYSQLDATA")
mysqlUser = os.getenv("MYSQLUSER")
mysqlPass = os.getenv("MYSQLPASS")

def connectMysql ():
    try:
        connection = mysql.connector.connect(host=mysqlHost,
                                            database=mysqlData,
                                            user=mysqlUser,
                                            password=mysqlPass)

        forgalom_Query = """ SELECT * FROM boltok """
        cursor = connection.cursor()
        result = cursor.execute(forgalom_Query)
        records = cursor.fetchall()
        """ for row in records:
            print(row) """

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " Error while connecting to MySQL " + f"{err}")
        errorMail(err)
        exit(1)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logging.info(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " MySQL connection is closed")