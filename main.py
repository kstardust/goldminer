import pandas
import sp500
import copy
import datetime
import logging


class GoldFilterBase:
    def __init__(self, Miner):
        self.miner = Miner
        self.input = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def GetOutput(self):
        # get filter output
        pass

    def Feed(self, data):
        self.input = data
        # input data
        pass


class GoldMiner:
    def __init__(self, sp500):
        self.filters = []
        self.sp500 = sp500
        self.intermediate_data = {}
        self.logger = logging.getLogger("GoldMiner")

    def SetIntermediateData(self, name, value):
        self.intermediate_data[name] = value

    def GetIntermediateData(self, name):
        return self.intermediate_data.get(name)

    def AddFilter(self, filter: GoldFilterBase):
        self.filters.append(filter)

    def Run(self):
        data = copy.deepcopy(self.sp500)
        for filter in self.filters:
            self.logger.info(f"run filter {filter.__class__.__name__}")
            filter.Feed(data)
            data = filter.GetOutput()

        return data


class PEFilter(GoldFilterBase):
    TENYEAR_TREASURY = 3.751 / 100

    def GetMaxPE(self):
        return 1 / self.TENYEAR_TREASURY * 0.8

    def GetOutput(self):
        passedTicker = {}
        maxPE = self.GetMaxPE()
        peSum = 0
        tickerCount = 0
        peCount = 0
        for ticker, data in self.input.items():
            summaryDetail = data.get('summaryDetail')

            if not summaryDetail:
                self.logger.error(f"cannot fetch summary of {ticker}")
                continue

            pe = summaryDetail.get('trailingPE')
            if pe is None:
                self.logger.error(f"cannot fetch meaningful PE of {ticker}")
                pe = None

            divRate = summaryDetail.get('dividendRate')
            if divRate is None:
                continue

            price = data['price']['regularMarketPrice']

            keyStat = data['defaultKeyStatistics']
            price2Book = keyStat.get('priceToBook')
            if not price2Book:
                self.logger.error(f"invalid price to book {ticker}")
                price2Book = None

            tickerCount += 1
            if pe:
                peCount += 1
                peSum += pe
                if pe <= maxPE:
                    self.logger.info(f'{ticker} passed, PE {pe}, divRate {divRate / price}, P2B {price2Book}')
                    passedTicker[ticker] = data

        self.logger.info(f"Total {tickerCount}, the average PE is {peSum / peCount}")
        return passedTicker


def PrintData(data, sp500Name):
    ticker2name = {t['Symbol']: t['Name'] for t in sp500Name}

    df = pandas.DataFrame(columns=['Sector', 'Name', 'Ticker', 'Price', 'Price2Book', 'PE', 'DivRate'])

    n = 0
    for ticker, tickerData in data.items():
        price = tickerData['price']['regularMarketPrice']
        price2Book = tickerData['defaultKeyStatistics'].get('priceToBook')
        pe = tickerData['summaryDetail']['trailingPE']
        dividendRate = tickerData['summaryDetail']['dividendRate'] / price
        sector = tickerData['summaryProfile']['sector']
        name = ticker2name.get(ticker, ticker)

        df.loc[n] = [sector, name, ticker, price, price2Book, pe, dividendRate]
        n += 1

    df.to_csv("sp500-2023-04-09.csv", index=False)


def main():
    SP500 = sp500.GoldMinerSP500Stats()
    if not SP500.UseDataofDate(datetime.datetime.today()):
        logging.getLogger("main").error("error no sp500 data")
        return

    Miner = GoldMiner(SP500.GetTickers())

    Miner.AddFilter(PEFilter(Miner))

    data = Miner.Run()
    PrintData(data, SP500.GetSP500())


if __name__ == '__main__':
    main()
