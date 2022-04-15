from pathlib import Path

import backtrader as bt

import pandas as pd
import numpy as np

from datetime import timedelta


class TestStrategy(bt.Strategy):
    params = (
        ("fibo", .169),
        ("hilo_range", (0., 999.),),
    )

    def __init__(self):
        self.high = self.datas[0].high
        self.low = self.datas[0].low
        self.resampledhigh = self.datas[1].high
        self.resampledlow = self.datas[1].low
        self.hilo = self.resampledhigh - self.resampledlow

        self.orders = []
        self.order_limits = []

    def notify_order(self, order):

        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:

            if order.isbuy():
                self.log(f"BUY ORDER EXECUTED, {order.__str__()}")

            elif order.issell():
                self.log(f"SELL ORDER EXECUTED, {order.__str__()}")

        elif order.status in [order.Margin, order.Rejected]:

            self.log(f"Order {order.ref} Margin/Rejected")

        elif order.status == order.Canceled:

            self.log(f"Order {order.ref} Canceled")

        elif order.status == order.Expired:

            self.log(f"Order {order.ref} expired")

        else:

            self.log(f"Other order event")

        if order in self.orders:
            self.orders.remove(order)

    def notify_trade(self, trade):

        if trade.isopen:

            self.log(f"POSITION {trade.ref} OPENED")

        elif trade.isclosed:

            self.log(f"POSITION {trade.ref} CLOSED, PROFIT (gross): {trade.pnl}, PROFIT (net): {trade.pnlcomm}")

        else:

            return

    def log(self, txt, dt=None):

        dt = dt or self.datas[0].datetime.datetime()

        print(f"{dt.isoformat(), txt}")

    def next(self):

        if not self.position:

            if self.datas[0].datetime.time().minute in [14, 29, 44, 59]:

                if self.resampledhigh[0] < self.resampledhigh[-1] and self.resampledlow[0] > self.resampledlow[-1]:

                    if self.params.hilo_range[0] <= self.hilo[0] <= self.params.hilo_range[1]:

                        if bool(self.orders):

                            self.log("New bar and no positions are open. Cancelling orders.")

                            for order in self.orders:
                                self.broker.cancel(order)

                        # Long bracket setup
                        stop_long = np.round(self.resampledhigh[0] * 4) / 4 + .25  # Parent order stop price

                        limit_long = np.round((self.resampledhigh[0]) * 4) / 4 + .25  # Parent order limit price

                        tp_stop_long = np.max([np.round((self.resampledhigh[0] + (
                                self.resampledhigh[0] - self.resampledlow[0]) * self.params.fibo) * 4) / 4,
                                               limit_long + .25])  # Take profit stop price

                        tp_limit_long = np.max([np.round((self.resampledhigh[0] + (
                                self.resampledhigh[0] - self.resampledlow[0]) * self.params.fibo) * 4) / 4,
                                                limit_long + .25])  # Take profit limit price

                        sl_stop_long = np.round(
                            (self.resampledhigh[0] - (self.resampledhigh[0] - self.resampledlow[0]) / 2) * 4
                        ) / 4 + .25  # Stop loss stop price

                        sl_limit_long = np.round(
                            (self.resampledhigh[0] - (self.resampledhigh[0] - self.resampledlow[0]) / 2) * 4
                        ) / 4  # Stop loss limit price

                        parent_long = self.buy(exectype=bt.Order.StopLimit, price=stop_long, plimit=limit_long,
                                               valid=timedelta(minutes=30), transmit=False)
                        take_profit_long = self.sell(exectype=bt.Order.StopLimit, price=tp_limit_long,
                                                     plimit=tp_stop_long, transmit=False, parent=parent_long)
                        stop_loss_long = self.sell(exectype=bt.Order.StopLimit, price=sl_limit_long,
                                                   plimit=sl_stop_long, transmit=True, parent=parent_long)

                        self.log(f"BUY BRACKET, parent stop: {stop_long}, parent limit: {limit_long}, "
                                 f"take profit stop: {tp_stop_long}, take profit limit: {tp_limit_long}, "
                                 f"stop loss stop: {sl_stop_long}, stop loss limit {sl_limit_long}")

                        # Short bracket setup
                        stop_short = np.round(self.resampledlow[0] * 4) / 4 - .25

                        limit_short = np.round((self.resampledlow[0]) * 4) / 4 - .25

                        tp_stop_short = np.min([np.round((self.resampledlow[0] - (
                                    self.resampledhigh[0] - self.resampledlow[0]) * self.params.fibo) * 4) / 4,
                                                 limit_short - .25])

                        tp_limit_short = np.min([np.round((self.resampledlow[0] - (
                                    self.resampledhigh[0] - self.resampledlow[0]) * self.params.fibo) * 4) / 4,
                                                 limit_short - .25])

                        sl_stop_short = np.round(
                            (self.resampledlow[0] + (self.resampledhigh[0] - self.resampledlow[0]) / 2) * 4
                        ) / 4 - .25

                        sl_limit_short = np.round(
                            (self.resampledlow[0] + (self.resampledhigh[0] - self.resampledlow[0]) / 2) * 4
                        ) / 4

                        parent_short = self.sell(price=stop_short, exectype=bt.Order.StopLimit, plimit=limit_short,
                                                 valid=timedelta(minutes=30), transmit=False)
                        take_profit_short = self.buy(price=tp_limit_short, exectype=bt.Order.StopLimit,
                                                     plimit=tp_stop_short, transmit=False, parent=parent_short)
                        stop_loss_short = self.buy(price=sl_limit_short, exectype=bt.Order.StopLimit,
                                                   plimit=sl_stop_short, transmit=True, parent=parent_short)

                        self.log(f"SELL BRACKET, parent stop: {stop_short}, parent limit: {limit_short}, "
                                 f"take profit stop: {tp_stop_short}, take profit limit: {tp_limit_short}, "
                                 f"stop loss stop: {sl_stop_short}, stop loss limit: {sl_limit_short}")

                        self.orders.extend([parent_long, take_profit_long, stop_loss_long,
                                            parent_short, take_profit_short, stop_loss_short])

        elif self.position.size > 0:

            if not bool(self.orders):

                self.log("A long position is open and there are no active orders. Forced position close.")
                self.close()

            else:

                limits = []

                for order in self.orders:
                    if order.issell():
                        limits.append(order.price)

                if len(limits) > 2:
                    limits.remove(np.min(limits))  # Removing the lowest limit as it belongs to short bracket

                self.log(f"High: {self.high[0]}, Low: {self.low[0]}, Limits: {limits}")

                if self.high[0] < np.min(limits) or self.low[0] > np.max(limits):

                    self.log("Market escaped w/o triggering limits. Forced position close")
                    self.close()

                    for order in self.orders:
                        self.broker.cancel(order)

        else:

            if not bool(self.orders):

                self.log("A short position is open and there are no active orders. Forced position close.")
                self.close()

            else:

                limits = []

                for order in self.orders:
                    if order.isbuy():
                        limits.append(order.price)

                if len(limits) > 2:
                    limits.remove(np.max(limits))  # Removing the highest limit as it belongs to long bracket

                self.log(f"High: {self.high[0]}, Low: {self.low[0]}, Limits: {limits}")

                if self.high[0] < np.min(limits) or self.low[0] > np.max(limits):

                    self.log("Market escaped w/o triggering limits. Forced position close")
                    self.close()

                    for order in self.orders:
                        self.broker.cancel(order)


if __name__ == "__main__":
    cerebro = bt.Cerebro()

    cerebro.addstrategy(TestStrategy)

    root = Path(".", "data", "clean")  # Specify path to your file
    my_data: pd.DataFrame = pd.read_hdf(str(root) + "/2021_all_spy_1min.h5", "df")
    my_data = my_data.reindex(
        index=pd.date_range(
            start=my_data.index[0],
            end=my_data.index[-1],
            freq='1 min'
        ),
        method='ffill'
    )

    my_aux_data = my_data.resample("15T").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }
    )
    my_aux_data.dropna(inplace=True)
    my_aux_data = my_aux_data.reindex(
        index=pd.date_range(
            start=my_data.index[0],
            end=my_data.index[-1],
            freq='1 min'
        ),
        method='ffill'
    )
    my_aux_data = my_aux_data.shift(15).shift(-1)

    my_data = my_data.loc['2021-01']
    my_aux_data = my_aux_data.loc['2021-01']

    data_feed = bt.feeds.PandasData(dataname=my_data)
    aux_feed = bt.feeds.PandasData(dataname=my_aux_data)

    cerebro.adddata(data_feed)
    cerebro.adddata(aux_feed)

    cerebro.broker.setcash(100000)
    cerebro.addsizer(bt.sizers.FixedSize, stake=50)

    # cerebro.broker.setcommission(commission=1, margin=0., mult=1)

    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trade_analysis")
    cerebro.addwriter(bt.WriterFile, csv=False)

    portfolio_start = cerebro.broker.getvalue()

    strats = cerebro.run()
    strat = strats[0]

    portfolio_end = cerebro.broker.getvalue()
    results = strat.analyzers.trade_analysis.get_analysis()

    print(f"==================== SUMMARY ====================\n"
          f"\n++++++++++++++++++++ Portfolio +++++++++++++++++++\n"
          f"Starting portfolio value: {portfolio_start}\n"
          f"Final portfolio value: {portfolio_end}\n"
          f"\n++++++++++++++++++ Trade totals ++++++++++++++++++\n"
          f"Total trades: {results['total']['total']}\n"
          f"    - thereof still open: {results['total']['open']}\n"
          f"    - thereof winners: {results['won']['total']}\n"
          f"    - thereof losers: {results['lost']['total']}\n"
          f"    - thereof long positions: {results['long']['total']}\n"
          f"    - thereof short positions: {results['short']['total']}\n"
          f"Win rate (%): {results['won']['total'] / results['total']['total'] * 100}\n"
          f"\n++++++++++++++++++ PnL totals ++++++++++++++++++\n"
          f"--------------------- gross ---------------------\n"
          f"Total PnL (gross): {results['pnl']['gross']['total']}\n"
          f"Average PnL (gross): {results['pnl']['gross']['average']}\n"
          f"---------------------- net ----------------------\n"
          f"Total PnL (net): {results['pnl']['net']['total']}\n"
          f"Average PnL (net): {results['pnl']['net']['average']}\n"
          f"-------------------- winners --------------------\n"
          f"Total PnL (winners): {results['won']['pnl']['total']}\n"
          f"Average PnL (winners): {results['won']['pnl']['average']}\n"
          f"Max PnL (winners): {results['won']['pnl']['max']}\n"
          f"--------------------- losers --------------------\n"
          f"Total PnL (losers): {results['lost']['pnl']['total']}\n"
          f"Average PnL (losers): {results['lost']['pnl']['average']}\n"
          f"Max PnL (losers): {results['lost']['pnl']['max']}\n"
          f"---------------------- longs ---------------------\n"
          f"Total PnL (longs): {results['long']['pnl']['total']}\n"
          f"Average PnL (longs): {results['long']['pnl']['average']}\n"
          f"--------------------- shorts ---------------------\n"
          f"Total PnL (shorts): {results['short']['pnl']['total']}\n"
          f"Average PnL (shorts): {results['short']['pnl']['average']}\n"
          f"\n++++++++++++++++++ Time totals ++++++++++++++++++\n"
          f"--------------------- general --------------------\n"
          f"Total time in market (# of bars): {results['len']['total']}\n"
          f"Average time in market (# of bars): {results['len']['average']}\n"
          f"Max time in market (# of bars): {results['len']['max']}\n"
          f"Min time in market (# of bars): {results['len']['min']}\n"
          f"--------------------- winners --------------------\n"
          f"Total time in market (# of bars): {results['len']['won']['total']}\n"
          f"Average time in market (# of bars): {results['len']['won']['average']}\n"
          f"Max time in market (# of bars): {results['len']['won']['max']}\n"
          f"Min time in market (# of bars): {results['len']['won']['min']}\n"
          f"--------------------- losers --------------------\n"
          f"Total time in market (# of bars): {results['len']['lost']['total']}\n"
          f"Average time in market (# of bars): {results['len']['lost']['average']}\n"
          f"Max time in market (# of bars): {results['len']['lost']['max']}\n"
          f"Min time in market (# of bars): {results['len']['lost']['min']}\n")
