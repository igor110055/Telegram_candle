import logging
import json
from binance.lib.utils import config_logging
from binance.spot import Spot as Client

class Binance_api_handler():
    """Handles retrieving information from Binance.

    Returns:
        Spot: Binance Spot Client
    """
    
    spot_client:Client = None
    
    def __init__(self, log_level=logging.INFO, log_file_name="log") -> None:
        config_logging(logging, log_level, log_file_name)
        logging.debug("Spot client created")
        self.spot_client = Client()
    
    def __enter__(self):
        return self.spot_client
    
    def get_klines_as_list(self, symbol, interval, **klineArguments) -> list:
        """Uses spot client to get historical Kline data.

        Args:
            symbol (str): Binance symbol; coin trading name.
            interval (str): kline interval
        Keyword Args:
            limit (int, optional): limit the results. Default 500; max 1000.
            startTime (int, optional): Timestamp in ms to get aggregate trades from INCLUSIVE.
            endTime (int, optional): Timestamp in ms to get aggregate trades until INCLUSIVE.
            
        Returns:
            list: Klines data.
        """
        klines_data = self.spot_client.klines(symbol, interval, **klineArguments)
        logging.info("Klines data retrieved from Binance. Returned as Python List")
        return klines_data
    
    def get_klines_as_json(self, symbol, interval, **klineArguments) -> str:
        """Calls binance_api_handler.get_klines_as_list to get the Klines data.

        Args:
            symbol (str): Binance symbol; coin trading name.
            interval (str): kline interval
        Keyword Args:
            limit (int, optional): limit the results. Default 500; max 1000.
            startTime (int, optional): Timestamp in ms to get aggregate trades from INCLUSIVE.
            endTime (int, optional): Timestamp in ms to get aggregate trades until INCLUSIVE.

        Returns:
            str: json string.
        """
        
        logging.debug("Klines data retrieved from Binance. Returned as json string")
        return json.dumps(self.get_klines_as_list(symbol, interval, **klineArguments))
    
    def get_klines_save_json(self, symbol, interval, 
                        json_file_name="history_klines", 
                        **klineArguments) -> None:
        """Calls binance_api_handler.get_klines_as_json to get the klines data.
        And save it as .json file in current PWD.

        Args:
            symbol (str): Binance symbol; coin trading name.
            interval (str): kline interval.
            json_file_name (str, optional): json file name. Defaults to "history_klines".
        Keyword Args:
            limit (int, optional): limit the results. Default 500; max 1000.
            startTime (int, optional): Timestamp in ms to get aggregate trades from INCLUSIVE.
            endTime (int, optional): Timestamp in ms to get aggregate trades until INCLUSIVE.
        """
        
        klines_data_json = self.get_klines_as_json(symbol, interval, **klineArguments)
        
        with open(json_file_name, "w") as kline_file:
            json.dump(klines_data_json, kline_file)
        
        logging.info("Klines data saved in json file. FILE NAME: "+json_file_name)
        
    def __exit__(self):
        logging.debug("Spot client destroyed")
