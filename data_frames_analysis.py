from dataclasses import dataclass
from pandas import DataFrame

@dataclass
class BollingerBandsIndicator():
    """Data class for Bollinger Bands Indicator.
    """
    upper_band: DataFrame
    lower_band: DataFrame
    buyer_points: DataFrame
    seller_points: DataFrame
    rolling_average: DataFrame
    close_band: DataFrame
    width: int
    
class KlinesAnalysis():
    """Provides miscellaneous functions for klines.
    """
    
    @staticmethod
    def get_rolling_average(klines:DataFrame, observations_window:int, columnName:str) -> DataFrame:
        """Calculates the rolling average using fixed observations window.

        Args:
            klines (DataFrame): Klines data.
            observations_window (int): Size of observation window. i.e. Moving window size.
            columnName (str): On what column should the rolling average should be calculated over.

        Returns:
            DataFrame: Return the Klines with only the original index and the average. Other columns are droped.
        """
        
        klines_droped_columns = klines[[columnName]]
        
        rolling_average = klines_droped_columns.rolling(window=observations_window).mean().dropna()
        
        return rolling_average
    
    @staticmethod
    def get_rolling_standard_deviation(klines:DataFrame, observations_window:int, columnName:str) -> DataFrame:
        """Calculates the rolling standard deviation using fixed observations window.

        Args:
            klines (DataFrame): Klines data.
            observations_window (int): Size of observation window. i.e. Moving window size.
            columnName (str): On what column should the rolling standard deviation should be calculated over.

        Returns:
            DataFrame: Return the Klines with only the original index and the standard deviation. 
            Other columns are dropped.
        """
        
        klines_droped_columns = klines[[columnName]]
        
        rolling_standard_deviation = klines_droped_columns.rolling(window=observations_window).std().dropna()
        
        return rolling_standard_deviation
        
        

class BollingerBands():
    """Convert Klines data to Bollinger Bands: lower bands, upper bands, buyer points,
    and seller points.
    """
    
    @staticmethod
    def get_bollinger_bands(klines:DataFrame, width:int= 2, observations_window:int= 20) -> BollingerBandsIndicator:
        """Calculates the indicators.

        Args:
            klines (DataFrame): Klines Data.
            width (int, optional): Bollinger Band Width. Defaults to 2.
            observations_window (int): Size of observation window. i.e. Moving window size. Defaults to 20.

        Returns:
            BollingerBandsIndicators: Contains the indicators.
        """
        
        rolling_average = KlinesAnalysis.get_rolling_average(klines, observations_window, "Close")
        rolling_standard_deviation = KlinesAnalysis.get_rolling_standard_deviation(klines, 
                                                    observations_window, "Close")
        
        upper_band = rolling_average + width * rolling_standard_deviation
        lower_band = rolling_average - width * rolling_standard_deviation
        upper_band = upper_band.rename(columns={'Close': 'upper'})
        lower_band = lower_band.rename(columns={'Close': 'lower'})
        
        bb = klines.join(upper_band).join(lower_band)
        bb = bb.dropna()
        buyers = bb[bb['Close'] <= bb['lower']]
        sellers = bb[bb['Close'] >= bb['upper']]
        
        bb_indicator = BollingerBandsIndicator(upper_band, lower_band,
                                               buyers, sellers,
                                               rolling_average, klines[["Close"]],
                                               width)
        
        return bb_indicator
        
        


    