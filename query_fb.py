from errormail import *
from datetime import datetime
import logging
import fdb
from dotenv import load_dotenv

load_dotenv()



logging.basicConfig(filename="log/logfile.log",
                    encoding='utf-8', level=logging.INFO)


def queryFb(fetchType, query, trafik):
    
    # enviroment variables setup
    fbHost = os.getenv("FBHOST")
    fbData = os.getenv("FBDATA")
    fbUser = os.getenv("FBUSER")
    fbPass = os.getenv("FBPASS")

    if trafik:
        fbHost = os.getenv("FBHOSTTRAF")

    try:
        connection = fdb.connect(
            host=fbHost,
            database=fbData,
            user=fbUser,
            password=fbPass,
            charset='utf-8'
        )
        cursor = connection.cursor()

        cursor.execute(query)

        if fetchType == "one":
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()

        connection.close()
        return (result)

    except Exception as err:
        logging.error(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') +
                      " Error while connecting to Firebird-SQL" + f" {query}"  + f" {err}")
        errorMail(err)
        exit(1)
