from errormail import *
from datetime import datetime
import logging
import fdb
from dotenv import load_dotenv

load_dotenv()

# enviroment variables setup
fbHost = os.getenv("FBHOST")
fbData = os.getenv("FBDATA")
fbUser = os.getenv("FBUSER")
fbPass = os.getenv("FBPASS")

logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)


def queryFb(boltok, fetchType, query):
    try:
        connection = fdb.connect(
            host=fbHost, database=fbData,
            user=fbUser, password=fbPass
        )
        cursor = connection.cursor()

        cursor.execute(query)

        if fetchType == "one":
            for row in cursor.fetchone():
                result = row
        else:
            result = cursor.fetchall()

        connection.close()
        return (result)

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                      " Error while connecting to Firebird-SQL" + f" {query}"  + f" {err}")
        errorMail(err)
        exit(1)
