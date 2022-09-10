import csv
import datetime
import logging
import mysql.connector

from errormail import *

logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)


def getLastaruMysql(mysqlHost, mysqlData, mysqlUser, mysqlPass):

    try:
        connection = mysql.connector.connect(host=mysqlHost,
                                             database=mysqlData,
                                             user=mysqlUser,
                                             password=mysqlPass,
                                             auth_plugin='mysql_native_password')

        cursor = connection.cursor()
        cursor.execute("SELECT max(id) FROM cikk")

        for row in cursor.fetchone():
            if row is None:
                row = "0"

            lastID = row

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                      " Error while connecting to MySQL " + f"{err}")
        errorMail(err)
        exit(1)

    cursor.close()
    connection.close()

    return (lastID)
