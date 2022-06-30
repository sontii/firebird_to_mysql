from ast import arg
import sys
import datetime
from datetime import date, timedelta
import fdb
import mysql.connector
from mysql.connector import Error

def validate(date_text):
        try:
            datetime.datetime.strptime(date_text, '%Y.%m.%d')
        except ValueError:
            raise ValueError("dátum formátum: YYYY.MM.DD")

def main():

    yesterday = date.today() - timedelta(days=1)

    if len(sys.argv) == 3:
        startDate = sys.argv[1]
        endDate = sys.argv[2]
        validate(startDate)
        validate(endDate)
    else:
        startDate = yesterday.strftime("%Y.%m.%d")
        endDate = startDate


    """ firebird  """
    try:
        con = fdb.connect(
            host='192.168.103.51', database='D:\Program Files\Laurel Kft\AIR Application\database\ibukr.gdb',
            user='SYSDBA', password='masterkey' 
        )
        cur = con.cursor()
        SELECT = "select * from AFA"
        cur.execute(SELECT)
        """ for row in cur:
            print(row) """

    except Error as e:
        print("Error while connecting to Firebird-SQL", e)


    """ mysql """

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
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    main()