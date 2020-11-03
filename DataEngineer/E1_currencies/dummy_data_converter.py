import psycopg2
from create_postgresql_db import DatabaseBuilder
import create_postgresql_db
import loadRatesToDB_async_postgresql


#Convert multiple source (target) prices into at least 2 base currencies: USD or EUR


class DataConverter(DatabaseBuilder):

    def __init__(self):
        self.conn = None        # connection
        self.cur = None         # cursor


    def create_connection(self):
        self.conn = psycopg2.connect(
        database="currency_dashboard", user='postgres', password='password', host='127.0.0.1', port= '5432')
        #Creating a cursor object using the cursor() method
        self.cur = self.conn.cursor()

        print("Connection established........")
    

    def get_cursor(self):
        ''' Create cursor '''

        self.cur = self.conn.cursor()


    def close_connection(self):
        self.conn.close()    


    def convert(self):

        #Make conversion to USD
        self.cur.execute("SELECT SUM(original_value) FROM OriginalValues")
        myvalue = self.cur.fetchall()
        a=myvalue[0]

        self.cur.execute("SELECT CAD FROM HistoricExchangeRates WHERE lookup_date = '2020-01-01' AND base_currency_code = 'USD' ")
        myrate_USD = self.cur.fetchall()
        b=myrate_USD[0]

        self.cur.execute("SELECT CAD FROM HistoricExchangeRates WHERE lookup_date = '2020-01-01' AND base_currency_code = 'EUR' ")
        myrate_EUR = self.cur.fetchall()
        c=myrate_EUR[0]

        print("Dealer X has a total of " + "%.4f" % a[0] + " CAD, which on the 2020-01-01 was worth " + "%.4f" %(a[0]*b[0]) + " USD and " + "%.4f" %(a[0]*c[0]) + " EUR, respectively.")




def main():


    myconverter=DataConverter()
    myconverter.create_connection()
    myconverter.get_cursor()
    myconverter.convert()
    myconverter.close_connection()



if __name__ == '__main__':
    main()
    