from enum import Enum
import json

class Config_types(Enum):
    """Contains all types of configurations.
    """
    influx_db = 1

class Config_handler():
    """Handle reading config json files .
    """
    
    config = None
    
    def __init__(self, fileName:str = "config.json") -> None:
        with open(fileName, "r") as config_file:
            self.configs = json.load(config_file)
    
    def get_config(self, config_type:Config_types) -> dict:
        """Returns the configs for the specified type.

        Args:
            config_type (Config_types): Config Type. From Config_types.

        Returns:
            dict: Contains the configuration.
        """
        
        return self.configs[config_type.name]