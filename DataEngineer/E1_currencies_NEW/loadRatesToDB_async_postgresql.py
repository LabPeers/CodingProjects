import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import asyncio
import aiohttp
import psycopg2
import json
from random import randrange


class Downloader:
    ''' Downloader '''

    def __init__(self):
        self.conn = None        # connection
        self.cur = None         # cursor

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


    def close_connection(self):
        ''' Close connection to database '''

        if self.conn is not None:
            self.conn.close()     


    def write_to_database1(self,data):
        print("hello database")
        mysql='''INSERT INTO ExchangeRates (
            fk_dID,
            base_currency_code,
            currency_code,
            rate) VALUES 
            (%s,%s,%s,%s)'''

    
        self.cur.executemany(mysql,data)
        self.conn.commit()
        
        print("Values inserted...........")



    def write_to_database2(self,mydate):
        mysql='''INSERT INTO Dates (
            lookup_date ) VALUES 
            (%s)'''
        self.cur.executemany(mysql,mydate)
        self.conn.commit()
        
        print("Values inserted...........")



    # async def write_to_database(self,data_monthly):
    #     self.cur.executemany('INSERT INTO Historic_exchange_rates VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    #                 data_monthly)
    #     self.conn.commit()           


    async def insert_records(self,client):
        '''Insert multiple rows inside table named 'HistoricExchangeRates' '''

        lastday = [31,29,31,30,31,30,31,31,30,31]
        month = 0
        fk_dID = 0
        mybase = ['USD','EUR']
        for i in range(0,2): #Iterate through base currencies
            for l in range(0,len(lastday)):#Iterate through months
                new_date=[]
                month=l+1
                data_monthly=[]
                for k in range(1,lastday[l]+1):#Iterate through days of each month
                    myurl = "https://api.exchangeratesapi.io/2020-{}-{}?base={}".format(month,k,mybase[i])


                    # In case you get blocked, use:
                    # headers = [{"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"},    
                    #             {"User-Agent": "Mozilla/5.0 (Linux; Android 6.0; HTC One X10 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/61.0.3163.98 Mobile Safari/537.36"},
                    #             {"User-Agent":"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 6P Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36"},
                    #             {"User-Agent":"Mozilla/5.0 (Linux; Android 7.1.1; G8231 Build/41.2.A.0.219; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/59.0.3071.125 Mobile Safari/537.36"}]
                    # async with client.get(myurl,headers=headers[randrange(3)]) as response:
                    
                    async with client.get(myurl) as response:
                        assert response.status == 200
                        data = await response.read()
                        exchange_rates = json.loads(data.decode('utf-8'))
                        myrates=exchange_rates['rates']
                        if i==1:
                            myrates["EUR"]=1

                        lookup_date='2020-{}-{}'.format(str(month).zfill(2),str(k).zfill(2))
                        
                        self.write_to_database2([(lookup_date,)])
                        fk_dID+=1

                        for m in range(0,len(myrates)):

                        #data_monthly.append((fk_dID,exchange_rates['base']) + tuple(myrates.values()))
                            #data_monthly.append((fk_dID,exchange_rates['base'])+ list(myrates.items())[m])
                            data_monthly.append((fk_dID,mybase[i])+ list(myrates.items())[m])

                            #print((fk_dID,exchange_rates['base'])+ list(myrates.items())[m])


                self.write_to_database1(data_monthly)
   
                print("Month entered successfully")


        print('Records inserted succesfully')    


async def main():
    ''' Create connection with the database '''
    #os.remove("exchange_rates_async.db")
    t0 = time.time()
    mydb = Downloader()
    mydb.create_connection()
    mydb.get_cursor()
    #mydb.insert_records()
    loop = asyncio.get_running_loop()      
    async with aiohttp.ClientSession(loop=loop) as client:
        await asyncio.gather(mydb.insert_records(client))
    #mydb.run_insertion_loop()
    mydb.close_connection()
    t1=time.time()
    print("took {} s".format(t1-t0))


if __name__ == '__main__':
    asyncio.run(main())