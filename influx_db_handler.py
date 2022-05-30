from influxdb_client import InfluxDBClient, Point, WriteApi, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from config_handler import Config_handler, Config_types
from pandas import DataFrame

class Influx_db_handler():
    """Handle Influx Database clients.
    Note: Only SYNCHRONOUS is implemented.
    
    Returns:
        Influx_db_handler: Instance of Influx_db_handler.
    """
    
    client:InfluxDBClient = None
    _influx_config = None
    _write_api:WriteApi = None
    
    def __init__(self, write_mode= SYNCHRONOUS, configFile:str = "config.json") -> None:
        self._influx_config = Config_handler(configFile)
        self._influx_config = self._influx_config.get_config(Config_types.influx_db)
        
        self.client = InfluxDBClient(
            url=self._influx_config["url"],
            org=self._influx_config["org"],
            token=self._influx_config["token"]
        )

        self._write_api = self.client.write_api(write_options=write_mode)
        
    def write_point(self, point: Point, bucket:str = None) -> None:
        """Write the given point to the database.

        Args:
            point (Point): Point to be inserted.
            bucket (str, optional): Influx Bucket. Defaults to _influx_config["bucket"].
        """
        
        if bucket == None:
            bucket = self._influx_config["bucket"]
        
        self._write_api.write(
            bucket,
            self._influx_config["org"],
            point
        )
        
    def write_dataframe(self, data_frame: DataFrame, measurement_name:str, tag_columns:list, bucket:str = None):
        """Write Pandas DataFrame to the Database.

        Args:
            data_frame (DataFrame): Pandas DataFrame.
            measurement_name (str): InfluxDB measurement name.
            tag_columns (list): DataFrame Columns to be used as tags.
            bucket (str, optional): Influx Bucket. Defaults to _influx_config["bucket"].
        """
        
        if bucket == None:
            bucket = self._influx_config["bucket"]
        
        self._write_api.write(
            bucket,
            self._influx_config["org"],
            record = data_frame,
            data_frame_measurement_name = measurement_name,
            data_frame_tag_columns = tag_columns
        )
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type,exc_value, exc_traceback):
        self._write_api.close()
        self.client.close()