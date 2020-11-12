import psycopg2

class DatabaseBuilder:


    def __init__(self):
        self.conn = None        # connection
        self.cur = None         # cursor
    

    def create_database(self):
        ''' Create connection with database '''
        #establishing the connection
        self.conn = psycopg2.connect(
        database="postgres", user='postgres', password='password', host='127.0.0.1', port= '5432'
        )
        self.conn.autocommit = True

        #Creating a cursor object using the cursor() method
        self.cur = self.conn.cursor()

        #Preparing query to create a database
        mysql = '''CREATE database exchange_rates_dashboard''';

        #Creating a database
        self.cur.execute(mysql)
        print("Database created successfully........")


    def create_connection(self):
        self.conn = psycopg2.connect(
        database="exchange_rates_dashboard", user='postgres', password='password', host='127.0.0.1', port= '5432'
        )
        #Creating a cursor object using the cursor() method
        self.cur = self.conn.cursor()

        print("Connection established........")
    
    def get_cursor(self):
        ''' Create cursor '''

        self.cur = self.conn.cursor()


    def drop_table(self):

        mysql="DROP TABLE IF EXISTS ExchangeRates, Dates"
        self.cur.execute(mysql)   

    
    def create_table(self,mysql):
        #Dumping OriginalValues table if already exists.
        #self.cur.execute("DROP TABLE IF EXISTS OriginalValues")

        self.cur.execute(mysql)
        print("Table created successfully........")
        self.conn.commit()

    
    def insert_values(self,data):
        mysql='''INSERT INTO Dates(lookup_date) VALUES ('2019-12-31')'''
        self.cur.execute(mysql)
        #self.cur.executemany(mysql,data)
        self.conn.commit()
        
        print("Values inserted...........")


    def close_connection(self):
        self.conn.close()


def main():
    mydashboard=DatabaseBuilder()
    #mydashboard.create_database()
    mydashboard.create_connection()
    mydashboard.drop_table()
    mysql_Dates ='''CREATE TABLE IF NOT EXISTS Dates (
        dID SERIAL PRIMARY KEY,
        lookup_date VARCHAR(20)
        )'''
    mydashboard.create_table(mysql_Dates)

    #mydashboard.insert_values('2019-12-31')
    
    mysql_ExchangeRates = '''CREATE TABLE IF NOT EXISTS ExchangeRates (
            exID SERIAL PRIMARY KEY,
            fk_dID INT,
            base_currency_code CHAR(3),
            currency_code CHAR(3),
            rate NUMERIC,
            FOREIGN KEY (fk_dID)
                REFERENCES Dates (dID)
                ON UPDATE CASCADE ON DELETE CASCADE
        )'''
    mydashboard.create_table(mysql_ExchangeRates)

    mydashboard.close_connection()
    


if __name__ == '__main__':
    main()