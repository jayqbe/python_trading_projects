from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

from threading import Thread
import queue

import time
from dateutil import parser

import pandas as pd


class IBDataApp(EWrapper, EClient):
    def __init__(self, host, port, clientId):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.data_queue_dict = {}
        self.datetime_list = list()
        self.open_list = list()
        self.high_list = list()
        self.low_list = list()
        self.close_list = list()
        self.volume_list = list()

        self.connect(host=host, port=port, clientId=clientId)

        thread = Thread(target=self.run)
        thread.start()
        setattr(self, "_thread", thread)

    def error(self, reqId, errorCode, errorString):

        error_msg = f"Error. Id: {reqId}, Code: {errorCode}, Message: {errorString}"

        print(error_msg)

    def nextValidId(self, orderId):

        print("Connection successful. Connection time: %s, Next valid Id: %d" % (
            self.twsConnectionTime().decode("ascii"), orderId
        ))

    def historicalData(self, reqId, bar):

        self.data_queue_dict[reqId].put(bar)

    def historicalDataEnd(self, reqId, start, end):

        print(f"Finished receiving current batch of historical data. Start: {start}. End: {end}")

        while not self.data_queue_dict[reqId].empty():

            bar_data = self.data_queue_dict[reqId].get()

            self.datetime_list.append(parser.parse(bar_data.date))
            self.open_list.append(bar_data.open)
            self.high_list.append(bar_data.high)
            self.low_list.append(bar_data.low)
            self.close_list.append(bar_data.close)
            self.volume_list.append(bar_data.volume)

    def request_historical_data(self, reqId, contract, endDateTime, durationStr, barSizeSetting, whatToShow,
                                useRTH, formatDate, keepUpToDate):

        print(f"Requesting historical data for {contract.symbol} at {contract.exchange}")

        self.reqHistoricalData(
            reqId=reqId,
            contract=contract,
            endDateTime=endDateTime,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow=whatToShow,
            useRTH=useRTH,
            formatDate=formatDate,
            keepUpToDate=keepUpToDate,
            chartOptions=[]
        )

        if reqId not in self.data_queue_dict.keys():

            print("Setting up queue for reqId %d" % reqId)
            self.data_queue_dict[reqId] = queue.Queue()

        return reqId

    def data_to_dataframe(self):

        data = {
            "open": self.open_list,
            "high": self.high_list,
            "low": self.low_list,
            "close": self.close_list,
            "volume": self.volume_list
        }

        dataframe = pd.DataFrame(data, index=self.datetime_list)
        dataframe.sort_index(inplace=True)

        return dataframe


if __name__ == "__main__":

    app = IBDataApp("localhost", 4002, 0)

    time.sleep(2)

    contract = Contract()
    contract.symbol = "SPY"
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "ARCA"

    dt_range = pd.date_range(start="20220328 23:59:59", end="20220401 23:59:59", freq="24H")

    for dt in dt_range[::-1]:
        reqId = app.request_historical_data(
            reqId=1001,
            contract=contract,
            endDateTime=dt.strftime("%Y%m%d %H:%M:%S"),
            barSizeSetting="1 min",
            durationStr="1 D",
            whatToShow="TRADES",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False
        )

        time.sleep(2)

    app.disconnect()

    df = app.data_to_dataframe()
    df.to_hdf("my_historical_data.h5", key="df", mode="w")
