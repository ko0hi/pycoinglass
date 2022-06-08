__version__ = '0.1.0'

import os
import requests
import numpy as np
import pandas as pd

from collections import namedtuple
from datetime import datetime, timezone
from enum import Enum, auto

def fromtimestamp(ts):
    return datetime.fromtimestamp(ts / 1000, timezone.utc)

class Symbol(Enum):
    ALL = auto()
    BTC = auto()
    ETH = auto()
    EOS = auto()
    BCH = auto()
    LTC = auto()
    XRP = auto()
    BSV = auto()
    ETC = auto()
    TRX = auto()
    LINK = auto()

class Exchange(Enum):
    Bitmex = auto()
    Binance = auto()
    Bybit = auto()
    Okex = auto()
    Huobi = auto()
    FTX = auto()
    Deribit = auto()
    Kraken = auto()
    Bitfinex = auto()
    Phemex = auto()


class DataParser:
    @staticmethod
    def margin_market_capture(data):
        df = pd.DataFrame(data)
        df['timestamp'] = list(map(fromtimestamp, df['updateTime']))
        df.sort_values("exchangeName", inplace=True)
        df.reset_index(inplace=True)
        df.drop(columns=["exchangeLogo", "symbolLogo"], inplace=True)
        df.rename(columns={"index": "rank"}, inplace=True)
        return df

    @staticmethod
    def exchange_open_interest(data):
        df = pd.DataFrame(data['dataMap'])
        df['timestamp'] = list(map(fromtimestamp, data['dateList']))
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        df['price'] = data['priceList']
        return df

    @staticmethod
    def exchange_open_interest_chart(data):
        df = pd.concat([
            pd.DataFrame(map(fromtimestamp, data['dateList']), columns=['timestamp']),
            pd.DataFrame(data['priceList'], columns=['price']),
            pd.DataFrame(data['dataMap']),
        ], axis=1).set_index('timestamp').sort_index()
        return df

    @staticmethod
    def liquidation(data):
        df = pd.DataFrame(data)
        df['timestamp'] = list(map(fromtimestamp, df.dateList))
        df.set_index('timestamp', inplace=True)
        return df

    @staticmethod
    def liquidation_chart(data):
        data_all, data_list = [], {}
        for item in data:
            new_item = {}
            for k, v in item.items():
                if k == 'list':
                    data_list[fromtimestamp(item['createTime'])] = pd.DataFrame(v).set_index("exchangeName")
                    continue
                if k == 'createTime':
                    k = 'timestamp'
                    v = fromtimestamp(v)
                new_item[k] = v
            data_all.append(new_item)
        df = pd.DataFrame(data_all).set_index('timestamp').sort_index()
        df_by_exchange = pd.concat(data_list)
        df_by_exchange.index.names = ['timestamp', 'exchange']

        LiquidationChart = namedtuple('LiquidationChart', ('total', 'exchange'))

        return LiquidationChart(df, df_by_exchange)

    @staticmethod
    def liquidation_history(data):
        df = pd.DataFrame(data['list'])
        df.side.replace({1: "BUY", 2: "SELL"}, inplace=True)
        df['timestamp'] = list(map(fromtimestamp, df.createTime))
        df['timestamp_turnover'] = list(map(fromtimestamp, df.turnoverTime))
        df.drop(columns=["exchangeLogo", "symbolLogo"], inplace=True)
        return df

    @staticmethod
    def funding_rate_chart(data):
        datelist = list(map(fromtimestamp, data['dateList']))
        dfs = []
        # dataMap: predicted
        # frDataMap: following
        for k in ['dataMap', 'frDataMap']:
            df = pd.DataFrame(data[k])

            # officialのfunding rate chart apiはfrDataMapが１短い
            if len(df) == len(data['priceList']):
                df['price'] = data['priceList']
            if len(df) == len(datelist):
                df.index = datelist
                df.index.name = "timestamp"

            dfs.append(df)

        FundingRateChart = namedtuple('FundingRateCharts', ('predicted', 'following'))

        return FundingRateChart(*dfs)

    @staticmethod
    def long_short_chart(data):
        df = pd.DataFrame(data)
        df.rename(columns={
            "dateList": "timestamp",
            "longRateList": "longRate", "shortsRateList": "shortRate",
            "longShortRateList": "longShortRate",
            "priceList": "price",
        }, inplace=True)
        df['timestamp'] = list(map(fromtimestamp, df.timestamp))
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        return df

    @staticmethod
    def exchange_vol(data):
        df = pd.DataFrame(data['dataMap'])
        df['total'] = data['priceList']
        df.index = list(map(fromtimestamp, data['dateList']))
        return df


class API:
    BASE_URL = "https://open-api.coinglass.com/api/pro/v1"
    BASE_URL_FUTURE = BASE_URL + "/futures"
    BASE_URL_OPTION = BASE_URL + "/option"

    PERIODS = {
        "all": 0, "1m": 9, "5m": 3, "15m": 10, "30m": 11, "1h": 2, "4h": 1, "12h": 4,
        "24h": 5, "90d": 18
    }

    # EXCHANGE_OPEN_INTEREST_INTERVAL = {
    #     "all": 0, "1h": 2, "4h": 1, "12h": 4
    # }
    # LIQUIDATION_CHAR_TIME = {
    #     "1m": 9, "5m": 3, "15m": 10, "30m": 11, "4h": 1, "12h": 4, "90d": 18
    # }
    # LONG_SHORT_CHART_INTERVAL = {
    #     "1h": 2, "4h": 1, "12h": 4, "24h": 5
    # }
    # OPTION_OPEN_INTEREST_INTERVAL = {
    #     "all": 0, "1h": 2, "4h": 1, "12h": 4, "24h": 5
    # }

    def __init__(self, api_key=None):
        if api_key is None:
            api_key = os.getenv("COINGLASS_API_KEY")
            assert api_key is not None, "missing `COINGLASS_API_KEY`"

        self.api_key = api_key

    # official api (https://coinglass.github.io/API-Reference/#general-info)
    # 実際にサイトで叩かれているAPIと違う
    def exchange_open_interest_official(self, symbol, period="all", headers=None, return_df=True):
        """ https://www.coinglass.com/BitcoinOpenInterest (Total BTC Futures Open Interest)

        :param symbol:
        :param period:
        :param headers:
        :param return_df:
        :return:
        """
        assert symbol in Symbol.__members__
        assert period in self.PERIODS
        url = f"{API.BASE_URL_FUTURE}/openInterest"
        params = dict(symbol=symbol, interval=self.PERIODS[period])
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, pd.DataFrame)

    def exchange_open_interest_chart_official(self, symbol, period="all", headers=None, return_df=True):
        assert symbol in Symbol.__members__
        assert period in self.PERIODS
        url = f"{API.BASE_URL_FUTURE}/openInterest/chart"
        params = dict(symbol=symbol, interval=self.PERIODS[period])
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.exchange_open_interest_chart)

    def liquidation_official(self, symbol, exchange, headers=None, return_df=True):
        """ https://coinglass.github.io/API-Reference/#liquidation

        :param symbol:
        :param exchange:
        :param headers:
        :param return_df:
        :return:
        """
        assert symbol in Symbol.__members__
        assert exchange in Exchange.__members__
        url = f"{API.BASE_URL_FUTURE}/liquidation_chart"
        params = dict(symbol=symbol, exName=exchange)
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.liquidation)

    def liquidation_chart_official(self, symbol, period="all", headers=None, return_df=True):
        """ https://coinglass.github.io/API-Reference/#liquidation-chart

        :param symbol:
        :param period:
        :param headers:
        :param return_df:
        :return:
        """
        assert symbol in Symbol.__members__
        assert period in self.PERIODS
        url = f"{API.BASE_URL_FUTURE}/liquidation/detail/chart"
        params = dict(symbol=symbol, timeType=self.PERIODS[period])
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.liquidation_chart)

    def long_short_chart_official(self, symbol, period="5m", headers=None, return_df=True):
        """ https://coinglass.github.io/API-Reference/#long-short-chart

        :param symbol:
        :param period:
        :param headers:
        :param return_df:
        :return:
        """
        assert symbol in Symbol.__members__
        assert period in self.PERIODS
        assert period not in ["all", "1m"]
        url = f"{API.BASE_URL_FUTURE}/longShort_chart"
        params = dict(symbol=symbol, interval=self.PERIODS[period])
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.long_short_chart)

    def funding_rate_chart_official(self, symbol, interval="8h", t="U", headers=None, return_df=True):
        """ https://coinglass.github.io/API-Reference/#funding-rates-chart

        :param symbol:
        :param interval:
        :param t: C (Token) or U (USD)
        :param headers:
        :param return_df:
        :return:
        """
        assert symbol in Symbol.__members__
        url = f"{API.BASE_URL_FUTURE}/funding_rates_chart"
        params = dict(symbol=symbol, type=t, interval=self.PERIODS[interval])
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.funding_rate_chart)

    def exchange_vol_official(self, symbol, headers=None, return_df=True):
        """ https://coinglass.github.io/API-Reference/#exchange-vol

        :param symbol:
        :param headers:
        :param return_df:
        :return:
        """
        assert symbol in Symbol.__members__
        url = f"{API.BASE_URL_FUTURE}/vol/chart"
        params = dict(symbol=symbol)
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.exchange_vol)

    # TODO
    # def option_open_interest_official(self, symbol, headers=None):
    #     assert symbol in Symbol.__members__
    #     url = f"{API.BASE_URL_OPTION}/openInterest"
    #     params = dict(symbol=symbol)
    #     return self.__get(url, params.params, headers)
    #
    # def option_open_interest_chart_official(self, symbol, interval="all", headers=None):
    #     assert symbol in Symbol.__members__
    #     assert interval in self.PERIODS
    #     url = f"{API.BASE_URL_OPTION}/openInterest/history/chart"
    #     params = dict(symbol=symbol, interval=self.PERIODS[interval])
    #     return self.__get(url, params, headers)
    #
    # def option_exchange_vol_official(self, symbol, headers=None):
    #     assert symbol in Symbol.__members__
    #     url = f"{API.BASE_URL_OPTION}/vol/history/chart"
    #     params = dict(symbol=symbol)
    #     return self.__get(url, params, headers)

    # Webで叩かれているAPI
    def margin_market_capture(self, symbol, perp_or_future="perp", headers=None, return_df=True):
        """ https://www.coinglass.com/ (BTC futures market (real-time))

        :param symbol:
        :param perp_or_future:
        :param headers:
        :param return_df:
        :return:
        """
        assert symbol in Symbol.__members__
        if perp_or_future == "perp":
            perp_or_future = 1
        elif perp_or_future == "future":
            perp_or_future = 0
        else:
            raise RuntimeError(f"Unsupported: {perp_or_future}")
        url = "https://fapi.coinglass.com/api/futures/v2/marginMarketCap"
        params = dict(symbol=symbol, type=perp_or_future)
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, lambda data: DataParser.margin_market_capture(data[symbol]))

    def exchange_open_interest(self, symbol, period="all", currency="USD", headers=None, return_df=True):
        """ https://www.coinglass.com/BitcoinOpenInterest で叩かれているAPI。

        :param symbol:
        :param period:
        :param currency: "USD" or "BTC"
        :param headers:
        :param return_df:
        :return:

        >>> df_1min = api.exchange_open_interest("BTC", period="1m")
        >>> df_5min = api.exchange_open_interest("BTC", period="5m")
        >>> df_1min_to_5min = df_1min.resample("300S").first()
        >>> df_5min == df_1min_to_5min  # equal other than missing spans
        """
        assert symbol in Symbol.__members__
        assert period in self.PERIODS
        url = f"https://fapi.coinglass.com/api/openInterest/v3/chart"
        params = dict(symbol=symbol, timeType=self.PERIODS[period], currency=currency, type=0)
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.exchange_open_interest)

    def liquidation_chart(self, symbol, period="1m", headers=None, return_df=True):
        """ https://www.coinglass.com/LiquidationData (Total Liquidations)

        :param symbol:
        :param period:
        :param headers:
        :param return_df:
        :return:
        """
        url = "https://fapi.coinglass.com/api/futures/liquidation/chart"
        assert symbol in Symbol.__members__
        assert period in self.PERIODS
        assert period not in ["all"]
        params = dict(symbol=symbol, timeType=self.PERIODS[period])
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.liquidation_chart)

    def liquidation_history(self, symbol=None, side=None, page_size=100, page_num=1, headers=None, return_df=True):
        """ https://www.coinglass.com/LiquidationData (Historical Liquidations)

        :param symbol:
        :param side: 1 = "BUY" 2 = "SELL"
        :param page_size:
        :param page_num:
        :param headers:
        :param return_df:
        :return:
        """
        url = f"https://fapi.coinglass.com/api/futures/liquidation/order?pageSize={page_size}&volUsd=1000&pageNum={page_num}"
        params = dict()
        if symbol:
            params['symbol'] = symbol
        if side:
            if isinstance(side, str):
                side = 1 if side == "BUY" else 2
            params['side'] = side
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.liquidation_history)

    def long_short_chart(self, symbol, period="5m", headers=None, return_df=True):
        """ https://www.coinglass.com/LongShortRatio (BTC Long/Short Ratio)

        :param symbol:
        :param period:
        :param headers:
        :param return_df:
        :return:
        """
        assert symbol in Symbol.__members__
        assert period in self.PERIODS
        assert period not in ["all", "1m"]
        url = "https://fapi.coinglass.com/api/futures/longShortChart"
        params = dict(symbol=symbol, timeType=self.PERIODS[period])
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.long_short_chart)

    def funding_rate_chart(self, symbol, t="U", interval="h8", headers=None, return_df=True):
        """ https://www.coinglass.com/pro/fr/BTC

        :param symbol:
        :param t: "C" (token) or "U" (USD)
        :param interval:
        :param headers:
        :param return_df:
        :return:
        """
        assert interval in ["m1", "m5", "h8"]
        url = "https://fapi.coinglass.com/api/fundingRate/v2/history/chart"
        params = dict(symbol=symbol, type=t, interval=interval)
        resp = self.__get(url, params, headers)
        return self.__make_return(resp, return_df, DataParser.funding_rate_chart)

    def __get(self, url, params, headers):
        headers = self.__init_headers(headers)
        resp = requests.get(url, params=params, headers=headers)

        return resp

        if resp.status_code != 200:
            raise RuntimeError(f"{resp.content}")
        else:
            j = resp.json()
            if j['msg'] != 'success':
                raise RuntimeError(f"{j}")
            else:
                return j['data']

    def __init_headers(self, headers=None):
        headers = dict(coinglassSecret=self.api_key)
        if headers is not None:
            headers.update(headers)
        return headers

    @classmethod
    def __make_return(cls, resp, return_df, data_parser):
        if return_df:
            data = cls.__validate_response(resp)
            return data_parser(data)
        else:
            return resp

    @classmethod
    def __validate_response(cls, resp):
        if resp.status_code != 200:
            raise RuntimeError(f"{resp.content}")
        else:
            j = resp.json()
            if j['msg'] != 'success':
                raise RuntimeError(f"{j}")
            else:
                data = j['data']
                if len(data) == 0:
                    raise RuntimeError(f"Empty data")
                else:
                    return data
