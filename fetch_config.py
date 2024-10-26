from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict

@dataclass
class FetchConfig:
    # Bloomberg connection
    BLOOMBERG_HOST: str = 'localhost'
    BLOOMBERG_PORT: int = 8194
    
    # Batch processing
    BATCH_SIZE: int = 50
    DEFAULT_FIELDS: List[str] = None
    
    # Data parameters
    START_YEAR: int = 1985
    MIN_FORWARD_YEARS: int = 2  # Ensure we look at least 2 years forward
    MONTHS: str = 'FGHJKMNQUVXZ'  # Bloomberg month codes
    COMMODITIES: List[str] = None
    
    # Update settings
    LOOKBACK_DAYS: int = 5
    
    # Paths
    BASE_PATH: str = None
    
    def __post_init__(self):
        if self.DEFAULT_FIELDS is None:
            self.DEFAULT_FIELDS = ["PX_LAST", "PX_VOLUME"]
        
        if self.COMMODITIES is None:
            self.COMMODITIES = ['CL', 'CO', 'XB', 'HO', 'NG']
            
        if self.BASE_PATH is None:
            self.BASE_PATH = "."
    
    @property
    def END_YEAR(self) -> int:
        """Ensure we look enough years forward"""
        current_year = datetime.now().year
        return current_year + self.MIN_FORWARD_YEARS
            
    @property
    def RAW_DATA_PATH(self) -> str:
        return f"{self.BASE_PATH}/raw_data"
    
    @property
    def LOGS_PATH(self) -> str:
        return f"{self.BASE_PATH}/logs"