import psycopg2

class DatabaseBuilder:


    def __init__(self):
        self.conn = None        # connection
        self.cur = None         # cursor
    

    def create_database(self):
        ''' Create connection with database '''
        #establishing the connection
        self.conn = psycopg2.connect(
        database="currency_dashboard", user='postgres', password='password', host='127.0.0.1', port= '5432'
        )
        self.conn.autocommit = True

        #Creating a cursor object using the cursor() method
        self.cur = self.conn.cursor()

        #Preparing query to create a database
        mysql = '''CREATE database currency_dashboard''';

        #Creating a database
        self.cur.execute(mysql)
        print("Database created successfully........")


    def create_connection(self):
        self.conn = psycopg2.connect(
        database="currency_dashboard", user='postgres', password='password', host='127.0.0.1', port= '5432'
        )
        #Creating a cursor object using the cursor() method
        self.cur = self.conn.cursor()

        print("Connection established........")
    
    def get_cursor(self):
        ''' Create cursor '''

        self.cur = self.conn.cursor()


    def drop_table(self):

        mysql="DROP TABLE IF EXISTS OriginalValues, HistoricExchangeRates"
        self.cur.execute(mysql)   

    
    def create_table(self,mysql):
        #Dumping OriginalValues table if already exists.
        #self.cur.execute("DROP TABLE IF EXISTS OriginalValues")

        self.cur.execute(mysql)
        print("Table created successfully........")
        self.conn.commit()

    
    def insert_values(self,data):
        mysql='''INSERT INTO OriginalValues(original_value,currency) VALUES (%s,%s)'''
        self.cur.executemany(mysql,data)
        self.conn.commit()
        
        print("Values inserted...........")


    def close_connection(self):
        self.conn.close()


def main():
    mydashboard=DatabaseBuilder()
    #mydashboard.create_database()
    mydashboard.create_connection()
    mydashboard.drop_table()
    mysql_ExchangeRates = '''CREATE TABLE IF NOT EXISTS HistoricExchangeRates (
            rID SERIAL PRIMARY KEY,
            lookup_date VARCHAR(20),
            currency_rate_date VARCHAR(20),
            base_currency_code CHAR(3),
            CAD NUMERIC,HKD NUMERIC,ISK NUMERIC,PHP NUMERIC,DKK NUMERIC,HUF NUMERIC,
            CZK NUMERIC,GBP NUMERIC,RON NUMERIC,SEK NUMERIC,IDR NUMERIC,INR NUMERIC,
            BRL NUMERIC,RUB NUMERIC,HRK NUMERIC,JPY NUMERIC,THB NUMERIC,
            CHF NUMERIC,EUR NUMERIC,MYR NUMERIC,BGN NUMERIC,TRY NUMERIC,CNY NUMERIC,
            NOK NUMERIC,NZD NUMERIC,ZAR NUMERIC,USD NUMERIC,MXN NUMERIC,SGD NUMERIC,
            AUD NUMERIC,ILS NUMERIC,KRW NUMERIC,PLN NUMERIC
        )'''
    mydashboard.create_table(mysql_ExchangeRates)

    mysql_originalValues ='''CREATE TABLE IF NOT EXISTS OriginalValues (
        oID SERIAL PRIMARY KEY,
        original_value NUMERIC (14,4),
        currency CHAR(3)
        )'''
    mydashboard.create_table(mysql_originalValues)
    dummy_data=[(30.34,"CAD"),(23.34,"CAD"),(99.34,"CAD"),(69.34,"CAD")]
    mydashboard.insert_values(dummy_data)
    mydashboard.close_connection()
    


if __name__ == '__main__':
    main()
