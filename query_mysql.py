from errormail import *
from datetime import datetime
import logging
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)

mysqlHost = os.getenv("MYSQLHOST")
mysqlData = os.getenv("MYSQLDATA")
mysqlUser = os.getenv("MYSQLUSER")
mysqlPass = os.getenv("MYSQLPASS")


def queryMysql(fetchType, query):

    try:
        connection = mysql.connector.connect(host=mysqlHost,
                                             database=mysqlData,
                                             user=mysqlUser,
                                             password=mysqlPass,
                                             auth_plugin='mysql_native_password')

        cursor = connection.cursor()
        cursor.execute(query)

        if fetchType == "one":
            for row in cursor.fetchone():
                if row is None:
                    row = "0"
                result = row
        else:
            result = cursor.fetchall()

        cursor.close()
        connection.close()

        return (result)

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                      " Error while connecting to MySQL" + f" {query}" + f" {err}")
        errorMail(err)
        exit(1)
