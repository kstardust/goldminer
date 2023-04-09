import json

import pandas as pd
import sqlite3
import yahooquery
import requests
import multiprocessing
from multiprocessing.pool import ThreadPool
import datetime
import logging
import setting
from dataclasses import dataclass
from YFTickerProxyWrapper import TickerProxyWrapper


@dataclass
class Stock:
    Name: str
    Sector: str
    Symbol: str


class GoldMinerSP500Stats:

    COL_NAME = 'sp500_stats_'
    UselessTickerStatFields = ['upgradeDowngradeHistory']

    def __init__(self):
        self.database = sqlite3.connect("goldminer.db")
        self.cursor = self.database.cursor()
        self.logger = logging.getLogger('GoldMinerSP500Stats')
        self.sp500Json = None
        self.tickers = None
        self.InitDB()

    def InitDB(self):
        # create a table
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS {}
                     (symbol TEXT PRIMARY KEY NOT NULL,
                     value TEXT NOT NULL,
                     date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);'''.format(setting.DB_NAME))

        # create an index on the date column
        self.cursor.execute('''CREATE INDEX IF NOT EXISTS date_index
                     ON {} (date);'''.format(setting.DB_NAME))

        # commit the changes
        self.database.commit()

    def GetDB(self):
        return self.cursor

    def CommitDB(self):
        self.database.commit()

    def GetSP500(self):
        Resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', proxies={"https": setting.YF_PROXY})

        if not Resp.ok:
            self.logger.error(f"cannot get datapackage, ret: {Resp.status_code}")
            return None

        table = pd.read_html(Resp.text)
        df = table[0]

        sp500 = []
        for index, row in df.iterrows():
            sp500.append({
                'Symbol': row['Symbol'],
                'Name': row['Security'],
                'Sector': row['GICS Sector']
            })
        self.sp500Json = sp500
        return self.sp500Json

    def GetLogger(self):
        return self.logger

    def GetTickers(self):
        if self.tickers:
            return self.tickers

        sp500 = self.GetSP500()

        pool = ThreadPool(processes=multiprocessing.cpu_count() * 2)
        tickers = {}

        def fetch_task(stock):
            retries = 3

            name = stock['Name']
            symbol = stock['Symbol']
            sector = stock['Sector']
            self.logger.info(f'fetching stats for ${symbol}, Company Name: {name}, Sector: {sector}')
            while True:
                if retries == 0:
                    self.logger.error(f"max retries reached, exit.")
                    return {}
                try:
                    t = yahooquery.Ticker(symbol)
                    # yfinance is not working anymore
                    # t = TickerProxyWrapper(yfinance.Ticker(symbol), setting.YF_PROXY)
                    # result = t.stats()

                    # yahooquery.Ticker.all_modules[symbol] is equivalent to yfinance.Ticker.status
                    result = t.all_modules[symbol]
                    if not isinstance(result, dict):
                        retries -= 1
                        self.logger.error(f"invalid stat, {result}, retrie: {retries}")
                        continue

                    for key in self.UselessTickerStatFields:
                        result.pop(key, None)
                    return {symbol: result}
                except:
                    import traceback
                    e = traceback.format_exc()
                    self.logger.error(f"error occurred during fetching stat, retrie: {retries} \n{e}")
                    retries -= 1

        for stock in sp500:
            pool.apply_async(fetch_task, [stock], callback=lambda data: tickers.update(data))

        pool.close()
        pool.join()

        self.tickers = tickers
        return self.tickers

    def UseDataofDate(self, date):

        result = self.GetDB().execute("SELECT symbol, value FROM {} WHERE date(date) = ?".format(setting.DB_NAME), (date.strftime("%Y-%m-%d"),)).fetchall()
        tickers = {}
        for ticker in result:
            print(ticker)
            tickers.update({ticker[0]: json.loads(ticker[1])})

        if result:
            self.tickers = tickers
            return True

        return False

    def UpdateDatabase(self):
        '''
        update the lastest sp500 stats data from internet and save to database
        '''

        self.GetSP500()
        self.GetTickers()
        self.Save()

    def Save(self):
        if self.sp500Json is None or self.tickers is None:
            self.logger.error("cannot save, invalid data")
            return

        now = datetime.datetime.now()
        date = now.strftime("%Y-%m-%d")

        self.GetDB().executemany(
            "INSERT INTO {} VALUES (?, ?, ?)".format(setting.DB_NAME), [(k, json.dumps(v), date) for k, v in self.tickers.items()]
        )

        self.CommitDB()
        self.logger.info(f"data of {date} has been written to database.")


if __name__ == '__main__':
    GDSP500 = GoldMinerSP500Stats()
    GDSP500.UpdateDatabase()

