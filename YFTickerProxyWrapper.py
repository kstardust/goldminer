import yfinance

# yahoo have been blocked in CN, VPN / Proxy is necessary


class TickerProxyWrapper:
    def __init__(self, Ticker: yfinance.Ticker, Proxy=None):
        self.Ticker = Ticker
        self.Proxy = Proxy

    def history(self, *args, **kwargs):
        return self.Ticker.history(*args, proxy=self.Proxy, **kwargs)

    def stats(self):
        return self.Ticker.stats(self.Proxy)

    @property
    def isin(self):
        return self.Ticker.get_isin(self.Proxy)

    @property
    def major_holders(self):
        return self.Ticker.get_major_holders(self.Proxy)

    @property
    def institutional_holders(self):
        return self.Ticker.get_institutional_holders(self.Proxy)

    @property
    def mutualfund_holders(self):
        return self.Ticker.get_mutualfund_holders(self.Proxy)

    @property
    def dividends(self):
        return self.Ticker.get_dividends(self.Proxy)

    @property
    def splits(self):
        return self.Ticker.get_splits(self.Proxy)

    @property
    def actions(self):
        return self.Ticker.get_actions(self.Proxy)

    @property
    def shares(self):
        return self.Ticker.get_shares(self.Proxy)

    @property
    def info(self):
        return self.Ticker.get_info(self.Proxy)

    @property
    def calendar(self):
        return self.Ticker.get_calendar(self.Proxy)

    @property
    def recommendations(self):
        return self.Ticker.get_recommendations(self.Proxy)

    @property
    def earnings(self):
        return self.Ticker.get_earnings(self.Proxy)

    @property
    def quarterly_earnings(self):
        return self.Ticker.get_earnings(self.Proxy, freq='quarterly')

    @property
    def financials(self):
        return self.Ticker.get_financials(self.Proxy)

    @property
    def quarterly_financials(self):
        return self.Ticker.get_financials(self.Proxy, freq='quarterly')

    @property
    def balance_sheet(self):
        return self.Ticker.get_balancesheet(self.Proxy)

    @property
    def quarterly_balance_sheet(self):
        return self.Ticker.get_balancesheet(self.Proxy, freq='quarterly')

    @property
    def balancesheet(self):
        return self.Ticker.get_balancesheet(self.Proxy)

    @property
    def quarterly_balancesheet(self):
        return self.Ticker.get_balancesheet(self.Proxy, freq='quarterly')

    @property
    def cashflow(self):
        return self.Ticker.get_cashflow(self.Proxy)

    @property
    def quarterly_cashflow(self):
        return self.Ticker.get_cashflow(self.Proxy, freq='quarterly')

    @property
    def sustainability(self):
        return self.Ticker.get_sustainability(self.Proxy)

    @property
    def options(self):
        if not self.Ticker._expirations:
            self.Ticker._download_options(self.Proxy)
        return tuple(self.Ticker._expirations.keys())

    @property
    def news(self):
        return self.Ticker.get_news(self.Proxy)

    @property
    def analysis(self):
        return self.Ticker.get_analysis(self.Proxy)

    @property
    def earnings_history(self):
        return self.Ticker.get_earnings_history(self.Proxy)

    @property
    def earnings_dates(self):
        return self.Ticker.get_earnings_dates(self.Proxy)

