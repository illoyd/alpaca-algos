import argparse
from datetime import datetime
from typing import List, Union, MutableMapping

import alpaca_trade_api as trade_api
import numpy as np
import pandas as pd
import pytz

ABORT_IF_CLOSED = False
IMBALANCE_THRESHOLD = 1.8


class Quote:
    def __init__(self, _symbol: str, _bid: float, _ask: float, _bid_size: float, _ask_size: float, _timestamp):
        self.symbol = _symbol
        self.bid = _bid
        self.ask = _ask
        self.bid_size = _bid_size
        self.ask_size = _ask_size
        self.timestamp = _timestamp
        self.has_traded = False

    @property
    def spread(self):
        return round(self.ask - self.bid, 2)

    @staticmethod
    def from_data(data):
        return Quote(
            data.symbol,
            float(data.bid_price),
            float(data.ask_price),
            float(data.bid_size),
            float(data.ask_size),
            data.timestamp
        )

    def __repr__(self):
        return f'Quote({self.timestamp.strftime("%H:%M:%S.%s")} for {self.symbol}: {self.bid} {self.ask} {self.bid_size}/{self.ask_size}; traded? {self.has_traded})'


class Order:
    def __init__(self, _id: str, _symbol: str, _side: str, _quantity: float):
        self.id = _id
        self.symbol = _symbol.upper()
        self.side = _side.lower()
        self.quantity = _quantity
        self.filled_quantity: float = 0.0

    @staticmethod
    def from_data(data):
        return Order(
            data.order['id'],
            data.order['symbol'],
            data.order['side'],
            float(data.order['qty'])
        )

    def __repr__(self):
        return f'Order({self.id}: {self.side} {self.symbol} {self.filled_quantity}/{self.quantity})'

    @property
    def directional_quantity(self):
        return self.quantity * (1 if self.is_buy else -1)

    @property
    def is_filled(self):
        return self.quantity == self.filled_quantity

    @property
    def pending(self):
        return self.quantity - self.filled_quantity

    @property
    def is_buy(self):
        return self.side == 'buy'

    @property
    def is_sell(self):
        return self.side == 'sell'


class Strategy:
    def __init__(self, _symbol: str):
        self.runner = None
        self.symbol = _symbol

    @property
    def api(self):
        return self.runner.api

    @property
    def orders(self):
        return {order.id: order for order in self.runner.orders.values() if order.symbol == self.symbol}

    def start(self):
        pass

    def stop(self):
        pass

    def on_quote(self, quote: Quote):
        pass

    def on_trade(self, data):
        pass

    def on_trade_updates(self, event: str, order: Order, data):
        pass


class Runner:
    def __init__(self):
        self.strategies: List[Strategy] = []
        self.orders: MutableMapping[str, Order] = {}

        # Prepare API and connection
        self.connection = None
        self.api: Union[trade_api.REST, None] = None

    def add_strategy(self, *strategies: Strategy):
        for strategy in strategies:
            strategy.runner = self
            self.strategies.append(strategy)

    def start(self):
        # Prepare the API
        print("Creating API...")
        self.api = trade_api.REST()

        # Check if the market is open
        clock = self.api.get_clock()
        if ABORT_IF_CLOSED and clock.is_open is False:
            print(
                f'Markets are closed (now {clock.timestamp.strftime("%a %-d %b %H:%M:%S")}). Next open is {clock.next_open.strftime("%a %-d %b %H:%M:%S")}')
            return

        # Track when will close
        tz_ny = pytz.timezone('America/New_York')
        liquidate_at = clock.next_close - pd.Timedelta(minutes=5)
        print(f'Will liquidate positions at {liquidate_at.strftime("%a %-d %b %H:%M:%S")}.')

        # Prepare all strategies
        for strategy in self.strategies:
            strategy.start()

        async def on_quote(data):
            quote = Quote.from_data(data)
            # print(f'Received quote {quote}')
            for strategy in self.strategies:
                strategy.on_quote(quote)
            # If closing...
            if datetime.now(tz=tz_ny) >= liquidate_at:
                self.connection.stop()
                for strategy in self.strategies:
                    strategy.stop()

        async def on_trade(data):
            print(f'Received trade {data.symbol} {data.size} @ {data.price}')
            for strategy in self.strategies:
                strategy.on_trade(data)

        async def on_trade_updates(data):
            # print(f'Received order {data}')
            # Fetch or create the order based on the Order ID, then pass to strategy
            order_id = data.order['id']
            if order_id not in self.orders:
                self.orders[order_id] = Order.from_data(data)
            for strategy in self.strategies:
                strategy.on_trade_updates(data.event, self.orders[order_id], data)

        # Configure connection
        symbols = [strategy.symbol for strategy in self.strategies]
        print("Starting connection for", *symbols)
        self.connection = trade_api.Stream()
        self.connection.subscribe_quotes(on_quote, *symbols)
        self.connection.subscribe_trades(on_trade, *symbols)
        self.connection.subscribe_trade_updates(on_trade_updates)
        self.connection.run()


class TickTakerStrategy(Strategy):
    def __init__(self, _symbol: str, max_quantity: int = 500, quantity_per_trade: int = 100):
        super().__init__(_symbol)
        tz_ny = pytz.timezone('America/New_York')
        self.max_quantity = max_quantity
        self.quantity_per_trade = quantity_per_trade
        self.current_quote = Quote(self.symbol, 0, 0, 0, 0, datetime.now(tz=tz_ny))
        self.previous_quote = Quote(self.symbol, 0, 0, 0, 0, datetime.now(tz=tz_ny))
        self.position = 0
        self.level_changes = 0

    def start(self):

        # Get current position
        try:
            data = self.api.get_position(self.symbol)
            print(data)
            self.position = float(data.qty)
            print(f'Found {self.position} positions for {self.symbol}')
        except trade_api.rest.APIError as ex:
            print(f'No positions for {self.symbol} found (code: {ex.code} / {ex})')
            if ex.code == 40410000:
                self.position = 0
            else:
                raise

    def stop(self):
        # Liquidate position immediately
        self.api.close_position(self.symbol)

    def total_position(self):
        # print([order.directional_quantity for order in self.orders.values()])
        return self.position + sum([order.directional_quantity for order in self.orders.values()])

    @property
    def can_buy(self):
        return self.total_position() < self.max_quantity

    @property
    def buyable_quantity(self):
        return min(self.quantity_per_trade, self.max_quantity - self.total_position())

    @property
    def can_sell(self):
        return self.total_position() > 0

    @property
    def sellable_quantity(self):
        return min(self.quantity_per_trade, self.total_position())

    def on_quote(self, quote: Quote):
        if (
                self.current_quote.bid != quote.bid
                and self.current_quote.ask != quote.ask
                and quote.spread == 0.01
        ):
            self.previous_quote = self.current_quote
            self.current_quote = quote
            self.level_changes += 1
            print('Level change:', self.previous_quote, self.current_quote, flush=True)

    def on_trade(self, data):
        # Ignore this trade if...
        # It is for a different symbol
        if data.symbol != self.symbol:
            # print('Ignoring trade - not the right symbol')
            return

        # We already traded on this level
        if self.current_quote.has_traded:
            print('Ignoring trade - already traded at this level')
            return

        # OR the trade is too close to the quote update so may be stale (for the old quote)
        if data.timestamp <= self.current_quote.timestamp + pd.Timedelta(np.timedelta64(50, 'ms')):
            print('Ignoring trade - too recent')
            return

        # OR the trade size was too small
        if data.size < 100:
            print('Ignoring trade - too small')
            return

        quote = self.current_quote
        print(f'Current quote: {quote}')

        # Place a BUY order if...
        if (
                data.price == quote.ask
                and quote.bid_size > quote.ask_size * IMBALANCE_THRESHOLD
                and self.can_buy
        ):
            try:
                quote.traded = True
                self.api.submit_order(
                    symbol=self.symbol,
                    qty=str(self.buyable_quantity),
                    side='buy',
                    type='limit',
                    time_in_force='ioc',
                    limit_price=str(quote.ask)
                )

                print('Buy at', quote.ask, flush=True)

            except Exception as e:
                print(e)
        else:
            ask = data.price == quote.ask
            imbalance = quote.bid_size > quote.ask_size * IMBALANCE_THRESHOLD
            print(
                f'Ask? {ask}; Imbalance? {imbalance}; Can buy? {self.can_buy}')

        # Place a SELL order if...
        if (
                data.price == quote.bid
                and quote.ask_size > quote.bid_size * IMBALANCE_THRESHOLD
                and self.can_sell
        ):
            # Everything looks right, so we submit our sell at the bid
            try:
                quote.traded = True
                self.api.submit_order(
                    symbol=self.symbol,
                    qty=str(self.sellable_quantity),
                    side='sell',
                    type='limit',
                    time_in_force='ioc',
                    limit_price=str(quote.bid)
                )
                print('Sell at', quote.bid, flush=True)
            except Exception as e:
                print(e)
        else:
            bid = data.price == quote.bid
            imbalance = quote.ask_size > quote.bid_size * IMBALANCE_THRESHOLD
            print(
                f'Bid? {bid}; Imbalance? {imbalance}; Can sell? {self.can_sell}')

    def on_trade_updates(self, event, order: Order, data):
        # Ignore if not for the symbol we are watching
        if order.symbol != self.symbol:
            return

        # Order was filled or partially filled; update it and if complete, settle it
        if event == 'fill' or event == 'partial_fill':
            order.filled_quantity = float(data.order['filled_qty'])
            if order.is_filled:
                self.on_order_settled(order)

        # Cancelled or rejected
        if event == 'canceled' or event == 'rejected':
            self.on_order_cancelled(event, order)

    def on_order_settled(self, order: Order):
        print(f'Order settled {order}')
        self.position += order.directional_quantity
        del self.runner.orders[order.id]

    def on_order_cancelled(self, event, order: Order):
        print(f'Order {event} {order}')
        del self.runner.orders[order.id]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--symbol', type=str, default='SNAP',
        help='Symbol you want to trade.'
    )
    parser.add_argument(
        '--quantity', type=int, default=500,
        help='Maximum number of shares to hold at once. Minimum 100.'
    )
    parser.add_argument(
        '--key-id', type=str, default=None,
        help='API key ID',
    )
    parser.add_argument(
        '--secret-key', type=str, default=None,
        help='API secret key',
    )
    parser.add_argument(
        '--base-url', type=str, default=None,
        help='set https://paper-api.alpaca.markets if paper trading',
    )
    args = parser.parse_args()
    assert args.quantity >= 100
    runner = Runner()
    runner.add_strategy(
        TickTakerStrategy(args.symbol, args.quantity, 100),
        TickTakerStrategy('UVXY', args.quantity, 100)
    )
    runner.start()
