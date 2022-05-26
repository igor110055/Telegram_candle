import json
from datetime import datetime
import pandas

class KlinesDataTransform():
    
    DEFAULT_COLUMN_NAMES_ORDER = [
        "Open_time",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Close_time",
        "Quote_asset_volume",
        "Number_of_trades",
        "Buy_base_asset_volume",
        "Buy_quote_asset_volume",
        "ignore"
    ]

    @staticmethod
    def klines_list_to_dataframes(symbol:str, klines_data: list, 
                        column_names_order:list = DEFAULT_COLUMN_NAMES_ORDER) -> pandas.DataFrame:
        """Convert klines information list to pandas DataFrame.

        Args:
            symbol (str): Binance symbol; coin trading name.
            klines_data (list): Klines data in python list.
            column_names_order (list, optional): Column names ordered the same as kline list info. 
            Defaults to DEFAULT_COLUMN_NAMES_ORDER.

        Returns:
            DataFrame: DataFrame object with all klines.
        """
        
        klines_DataFrame = pandas.DataFrame(klines_data)
        klines_DataFrame.columns = column_names_order
        
        #Converting Binance timestamp. (ms -> date)
        klines_DataFrame["Open_time_date"] = [datetime.fromtimestamp(timestamp/1000) 
                                            for timestamp in klines_DataFrame.Open_time]
        klines_DataFrame["Close_time_date"] = [datetime.fromtimestamp(timestamp/1000) 
                                            for timestamp in klines_DataFrame.Close_time]
        
        klines_DataFrame["symbol"] = symbol
        
        klines_DataFrame = klines_DataFrame.set_index("Open_time_date")
        
        #Fixing the types of the values.
        klines_DataFrame = klines_DataFrame.astype({
            "Open" : float,
            "Close": float,
            "High": float,
            "Low": float
        })
        
        return klines_DataFrame

    @staticmethod
    def klines_json_string_to_dataframes(symbol:str, klines_json_string: str, 
                        column_names_order:list = DEFAULT_COLUMN_NAMES_ORDER) -> pandas.DataFrame:
        """Convert klines json string information to pandas DataFrame.

        Args:
            symbol (str): Binance symbol; coin trading name.
            klines_json_string (str): Klines data in json string.
            column_names_order (list, optional): Column names ordered the same as kline list info. 
            Defaults to DEFAULT_COLUMN_NAMES_ORDER.

        Returns:
            DataFrame: DataFrame object with all klines.
        """
        klines_python_list = json.loads(klines_json_string)
        return KlinesDataTransform.klines_list_to_dataframes(symbol, klines_python_list, column_names_order)

    @staticmethod
    def klines_json_file_to_dataframes(symbol:str, klines_json_file: str, 
                        column_names_order:list = DEFAULT_COLUMN_NAMES_ORDER) -> pandas.DataFrame:
        """Convert klines json file information to pandas DataFrame.

        Args:
            symbol (str): Binance symbol; coin trading name.
            klines_json_file (str): Klines json file name.
            column_names_order (list, optional): Column names ordered the same as kline list info. 
            Defaults to DEFAULT_COLUMN_NAMES_ORDER.

        Returns:
            DataFrame: DataFrame object with all klines.
        """
        
        with open(klines_json_file) as klines_file:
            klines_python_list = json.load(klines_file)
            
        return KlinesDataTransform.klines_list_to_dataframes(symbol, klines_python_list, column_names_order)

if __name__ == "__main__":
    # Simple test for loading klines info from json.
    from tabulate import tabulate
    klines_dataframes = KlinesDataTransform.klines_json_file_to_dataframes("BTC_USDT", 
                                "history_BTC_USDT.json")
    
    dataframes_in_table = tabulate(klines_dataframes, headers="keys", tablefmt='psql')
    
    print(dataframes_in_table)
    
    
    
    
    