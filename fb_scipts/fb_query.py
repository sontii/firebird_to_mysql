import datetime
import logging
import fdb

from errormail import *

logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)


def fbQuery(fbHost, fbData, fbUser, fbPass, querySelect):

    try:
        connection = fdb.connect(
            host=fbHost, database=fbData,
            user=fbUser, password=fbPass
        )
        cursor = connection.cursor()

        cursor.execute(querySelect)
        result = cursor.fetchall()
        fdb.Connection.close()
        return(result)

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                      " Error while connecting to Firebird-SQL " + f"{err}")
        errorMail(err)
        exit(1)
