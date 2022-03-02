import sys
import threading
from threading import Thread
import signal

import logging
import logging.config

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.utils import iswrapper

from datetime import datetime, timedelta
from dateutil import parser

import pytz

import time


# Define global variables
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "class": "logging.Formatter",
            "format": "%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "filename": yourfilename,  # Enter path where the logs should be stored
            "mode": "a",
            "formatter": "detailed"
        },
    },
    "loggers": {
        "MyPythonStrategy": {
            "level": "DEBUG",
            "handlers": ["console", "file"]
        }
    }
}
logger = None


# Define custom classes
class TradeableSecurity(Contract):
    def __init__(self, security_id: int, tradeable_hours: str = None, tradeable_hours_io: str = "in", **kwargs):
        Contract.__init__(self)
        self.security_id = security_id
        self.tradeableHours = tradeable_hours
        self.tradeableHoursIO = tradeable_hours_io
        self.__dict__.update(kwargs)

    def __repr__(self):

        return f"{vars(self)}"

    def update_security_details(self, security_detail_objects: list):

        for obj in security_detail_objects:

            for attr in vars(obj):
                setattr(self, attr, getattr(obj, attr))

    def is_trading_permitted(self):

        utc_now = datetime.now().astimezone(pytz.timezone("UTC"))

        if self.tradingHours.split(";")[0].split(":")[-1] == "CLOSED":
            logger.info(f"Exchange closed, no trading in {self.symbol}")

            return False

        elif self.tradeableHours:

            exchange_tz = pytz.timezone(self.timeZoneId)
            tradeable_hours_start = parser.parse(datetime.now().date().strftime("%Y%m%d") +
                                                 " " + self.tradeableHours.split("-")[0])
            tradeable_hours_end = parser.parse(datetime.now().date().strftime("%Y%m%d") +
                                               " " + self.tradeableHours.split("-")[-1])

            localized_tradeable_hours_start = exchange_tz.localize(tradeable_hours_start)
            localized_tradeable_hours_end = exchange_tz.localize(tradeable_hours_end)

            if self.tradeableHoursIO == "in":

                is_tradeable = localized_tradeable_hours_start < utc_now < localized_tradeable_hours_end

                logger.debug(f"Inside manually set trading hours for {self.symbol}: {is_tradeable}")

                return is_tradeable

            elif self.tradeableHoursIO == "out":

                is_tradeable = (utc_now < localized_tradeable_hours_start) or (
                        localized_tradeable_hours_end < utc_now)

                logger.debug(f"Inside manually set trading hours for {self.symbol}: {is_tradeable}")

                return is_tradeable

            else:

                logger.warning(f"Wrong value passed! Only 'in' or 'out' are permitted. Defaulting to 'in")

                is_tradeable = localized_tradeable_hours_start < utc_now < localized_tradeable_hours_end

                logger.debug(f"Inside manually set trading hours for {self.symbol}: {is_tradeable}")

                return is_tradeable

        else:

            exchange_tz = pytz.timezone(self.timeZoneId)
            liquid_hours_start = parser.parse(self.liquidHours.split(";")[0].split("-")[0].replace(":", " "))
            liquid_hours_end = parser.parse(self.liquidHours.split(";")[0].split("-")[-1].replace(":", " "))

            localized_liquid_hours_start = exchange_tz.localize(liquid_hours_start)
            localized_liquid_hours_end = exchange_tz.localize(liquid_hours_end)

            logger.debug(f"Inside liquid trading hours for {self.symbol}: "
                         f"{localized_liquid_hours_start < utc_now < localized_liquid_hours_end}")

            return localized_liquid_hours_start < utc_now < localized_liquid_hours_end


class MyBar:
    def __init__(self, date, high, low):
        self.date = date
        self.high = high
        self.low = low

    def __repr__(self):

        return f"{vars(self)}"

    def as_dict(self):

        return {"Date": self.date, "High": self.high, "Low": self.low}


class BarStream:
    def __init__(self, security_id):
        self.securityId = security_id  # Must be same as secId of related tradeable security
        self.current_bar = None
        self.finished_bars = []
        self.bar_finished = False

    def __repr__(self):

        return f"Object of class BarStream. Receives new market data and detects formation for secId: {self.securityId}"

    def update_bar_stream(self, new_bar: MyBar):

        if not bool(self.current_bar):

            self.current_bar = new_bar
            logger.debug(f"Starting a brand new bar stream for {ID_TO_SYMBOL_MAP[self.securityId]}: "
                         f"{self.current_bar}")

            self.bar_finished = False

        else:

            if new_bar.date == self.current_bar.date:

                self.current_bar = new_bar
                self.bar_finished = False

            else:

                self.finished_bars.append(self.current_bar)
                self.bar_finished = True

                self.current_bar = new_bar


class MyApp(EWrapper, EClient):
    def __init__(self, ipaddress, port_id, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.exit_event = threading.Event()
        self.next_valid_id = None

        self.tradeable_securities = {}
        self.bar_streams = {}

        self.__init_startup(ipaddress, port_id, client_id)

    def __init_startup(self, ipaddress, port_id, client_id):

        logger.info("Initializing connection to IB server...")
        self.connect(ipaddress, port_id, client_id)

        thread = Thread(target=self.run)
        thread.start()

        setattr(self, "_thread", thread)

        time.sleep(2)

        if not self.twsConnectionTime():
            logger.warning("Failed to connect to server. Exiting...")
            sys.exit()
        else:
            logger.info(f"Connection successful. Connection time: {self.twsConnectionTime().decode('utf-8')}")
            logger.debug(f"Received next valid Id from the server: {self.next_valid_id}")

    def add_tradeable_securities(self, tradeable_securities: [TradeableSecurity]):

        for security in tradeable_securities:

            self.tradeable_securities[security.security_id] = security
            logger.debug(f"Adding {security.symbol} to tradeable securities list with Id: {security.security_id}")

            self.add_bar_stream(security.security_id)

    def add_bar_stream(self, secId):

        self.bar_streams[secId] = BarStream(secId)
        logger.debug(self.bar_streams[secId])

    @iswrapper
    def error(self, reqId, errorCode, errorString):

        error_msg = f"Error. Id: {reqId}, Code: {errorCode}, Message: {errorString}"

        if errorCode == 202:
            logger.debug(f"Order {reqId} cancelled by the server. {error_msg}")
        else:
            logger.debug(error_msg)

    @iswrapper
    def nextValidId(self, orderId):

        self.next_valid_id = orderId

    def get_next_valid_order_id(self):

        oid = self.next_valid_id
        self.next_valid_id += 1

        return oid

    @iswrapper
    def contractDetails(self, reqId, contractDetails):

        logger.debug(f"Receiving contract details response for {self.tradeable_securities[reqId].symbol}")
        self.tradeable_securities[reqId].update_security_details([contractDetails, contractDetails.contract])

    @iswrapper
    def contractDetailsEnd(self, reqId):

        logger.debug(f"Finished receiveing contract details for {self.tradeable_securities[reqId].symbol}")
        logger.debug(f"{self.tradeable_securities[reqId].__repr__()}")

    def req_contract_details(self, tradeable_securities: dict):

        if not bool(tradeable_securities):
            logger.warning("There are no tradeable securities. Exiting the program...")
            self.disconnect()

        else:
            for k, v in tradeable_securities.items():

                self.reqContractDetails(k, v)

                logger.debug(f"Requesting contract details for {v.symbol} on {v.exchange}")
                time.sleep(2)

    @iswrapper
    def historicalData(self, reqId, bar):

        new_bar = MyBar(bar.date, bar.high, bar.low)

        logger.debug(f"Receiving bar data: {new_bar}")

        self.bar_streams[reqId].update_bar_stream(new_bar)

    @iswrapper
    def historicalDataUpdate(self, reqId, bar):

        new_bar = MyBar(bar.date, bar.high, bar.low)

        stream = self.bar_streams[reqId]
        stream.update_bar_stream(new_bar)

        """Here goes strategy execution"""

    @iswrapper
    def historicalDataEnd(self, reqId, start, end):

        logger.debug(f"Initial bar for {self.tradeable_securities[reqId].symbol} received.")

    def start_bars_stream(self):

        for k, v in self.tradeable_securities.items():

            self.reqHistoricalData(
                reqId=k,
                contract=v,
                endDateTime="",
                durationStr="900 s",
                barSizeSetting="15 mins",
                whatToShow="TRADES",
                useRTH=0,
                formatDate=1,
                keepUpToDate=True,
                chartOptions=[]
            )
            logger.info(f"Bar stream initiated for {v.symbol}")

            time.sleep(1)

    def signal_handler(self, *args):

        self.exit_event.set()

        self.disconnect()


def main():

    # Set up logging
    global logger
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger("MyPythonStrategy")

    # Start main body of the program
    logger.info("Executing main program")
    app = MyApp("", 1234, 0)  # Enter IP address, port and client ID

    # Define IB Contracts (can be more than 1)
    logger.info("Setting up tradeable securities")
    my_security_spy = TradeableSecurity(
        security_id=1001,
        tradeable_hours="0930-1600",
        symbol="SPY",
        secType="STK",
        currency="USD",
        exchange="ARCA",
        localSymbol="SPY"
    )

    tradeable_securities = [my_security_spy]
    app.add_tradeable_securities(tradeable_securities)

    app.req_contract_details(app.tradeable_securities)

    app.start_bars_stream()

    signal.signal(signal.SIGINT, app.signal_handler)


if __name__ == "__main__":

    main()
