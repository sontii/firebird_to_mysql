import sys
import csv
import datetime
from datetime import date, timedelta
import logging
import fdb
import mysql.connector
from mysql.connector import Error

logging.basicConfig(filename="log/logfile.log", encoding='utf-8', level=logging.INFO)


""" bolti lekérés
"select sum(bteny_ert), sum(nteny_ert), sum(nyilv_ert) from blokk_tet where datum = '" & Format(tegnapt, "yyyy.MM.dd") & "'"
-- """

""" központ lekérés
"select sum(bteny_ert), sum(nteny_ert), sum(nyilv_ert) from blokk_tet where datum = '" & Format(tegnapt, "yyyy.MM.dd") & "' and egyseg='" & boltkod & "'
-- """

boltok = ["001", "002", "003", "004", "005", "006", "007", "008", "009"]


""" check argv date is valid  """
def validate(date_text):
        try:
            datetime.datetime.strptime(date_text, '%Y.%m.%d')
        except ValueError:
            logging.error(" " + datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " dátum formátum: YYYY.MM.DD")
            exit(1)

def connectMysql ():
    try:
        connection = mysql.connector.connect(host='192.168.103.101',
                                            database='lazar',
                                            user='lazar',
                                            password='lazarteam')

        forgalom_Query = """ SELECT * FROM boltok """
        cursor = connection.cursor()
        result = cursor.execute(forgalom_Query)
        records = cursor.fetchall()
        """ for row in records:
            print(row) """

    except Error as e:
        logging.error(" " + datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " Error while connecting to MySQL", e)
        exit(1)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logging.info(" " + datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " MySQL connection is closed")


def connectFdb (startDate, endDate):
    file = open('temp.csv', 'w', encoding="UTF8", newline='')
    writer = csv.writer(file)
    try:
        con = fdb.connect(
            host='192.168.103.51', database='D:\Program Files\Laurel Kft\AIR Application\database\ibukr.gdb',
            user='SYSDBA', password='masterkey' 
        )
        cur = con.cursor()
        for bolt in boltok:
            SELECT = f"SELECT SUM(bteny_ert), SUM(nteny_ert), SUM(nyilv_ert) FROM blokk_tet WHERE datum between '{startDate}' AND '{endDate}' AND egyseg = '{bolt}'"

            cur.execute(SELECT)
            for row in cur:
                writer.writerow([bolt] + [row[0]] + [row[1]] + [row[2]])


    except Error as e:
        logging.error(" " + datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " Error while connecting to Firebird-SQL", e)
        exit(1)

    file.close()

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

    connectFdb (startDate, endDate)


if __name__ == "__main__":
    main()
