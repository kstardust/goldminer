import pandas as pd
import pymongo
import yfinance
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

    COL_NAME = 'sp500_stats'
    UselessTickerStatFields = ['upgradeDowngradeHistory']

    def __init__(self):
        self.mongoCli = pymongo.MongoClient(setting.MONGO_URI)
        self.database = self.mongoCli.get_database(setting.DB_NAME)
        self.logger = logging.getLogger('GoldMinerSP500Stats')
        self.sp500Json = None
        self.tickers = None

    def GetDBCol(self):
        return self.database[self.COL_NAME]

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
                    exit(-1)
                try:
                    t = TickerProxyWrapper(yfinance.Ticker(symbol), setting.YF_PROXY)
                    result = t.stats()
                    if not result:
                        retries -= 1
                        self.logger.error(f"invalid stat, retrie: {retries} \n{e}")
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

        if isinstance(date, datetime.datetime):
            date = date.strftime("%Y-%m-%d")

        result = self.GetDBCol().find_one({'date': date})
        if result:
            self.tickers = result['tickers']
            self.sp500Json = result['sp500']
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

        self.GetDBCol().update_one(
            {
                'date': date
            },
            {
                '$set': {
                    'sp500': self.sp500Json,
                    'tickers': self.tickers,
                    'save_time': now,
                    'date': date,
                }
            },
            upsert=True
        )
        self.logger.info(f"data of {date} has been written to database.")


if __name__ == '__main__':
    GDSP500 = GoldMinerSP500Stats()
    #GDSP500.UseDataofDate(datetime.datetime.now())

