import csv
import datetime
import logging
import mysql.connector

from errormail import *

logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)


def aruToMysql(mysqlHost, mysqlData, mysqlUser, mysqlPass):

    fileAru = open('aru_temp.csv', 'w', encoding="UTF8", newline='')
    writerAru = csv.writer(fileAru)

    try:
        connection = mysql.connector.connect(host=mysqlHost,
                                             database=mysqlData,
                                             user=mysqlUser,
                                             password=mysqlPass,
                                             auth_plugin='mysql_native_password')

        forgalom_Query = """ SELECT * FROM boltok """
        cursor = connection.cursor()
        result = cursor.execute(forgalom_Query)
        records = cursor.fetchall()
        """ for row in records:
            writerAru.writerow([bolt] + [row[0]] + [row[1]] + [row[2]]) """

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                      " Error while connecting to MySQL " + f"{err}")
        errorMail(err)
        exit(1)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logging.info(
                " " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " MySQL connection is closed")

    fileAru.close()
