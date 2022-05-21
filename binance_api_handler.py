import time
import logging
from binance.lib.utils import config_logging
from binance.websocket.spot.websocket_client import SpotWebsocketClient as Client

config_logging(logging, logging.DEBUG, "log")


def message_handler(message):
    logging.info(message)


my_client = Client(stream_url="wss://stream.binance.com:9443")
my_client.start()

my_client.kline(symbol="btcusdt", id=1, interval="1m", callback=message_handler)
my_client.kline(symbol="bnbusdt", id=2, interval="1m", callback=message_handler)

time.sleep(15)

logging.debug("closing ws connection")
my_client.stop()