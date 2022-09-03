import sys
import datetime
from datetime import date, timedelta
import logging

from fb_aru import *
from fb_forgalom import *
from my_aru import *

logging.basicConfig(filename="log/logfile.log", encoding='utf-8', level=logging.INFO)


# check argv date is valid
def validate(date_text):
        try:
            datetime.datetime.strptime(date_text, '%Y.%m.%d')
        except ValueError:
            logging.error(" " + datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " dátum formátum: YYYY.MM.DD")
            exit(1)



def main():

    yesterday = date.today() - timedelta(days=1)

    """ if we have argv """
    if len(sys.argv) == 3:
        startDate = sys.argv[1]
        endDate = sys.argv[2]
        validate(startDate)
        validate(endDate)
        if startDate > endDate:
            logging.error(" " + datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " A kezdő dátum nem lehet nagyobb mint a vég dátum")
            exit(1)
    else:

        startDate = yesterday.strftime("%Y.%m.%d")
        endDate = startDate

    getForgalom (startDate, endDate)

### TODO
### getAru (mysqlLastItem, fbLastItem)
### aruToMysql (csv -> to  mysql)
### forgalomToMysql ()





if __name__ == "__main__":
    main()
