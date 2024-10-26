from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

@dataclass
class SpreadsConfig:
    # Calculation parameters
    TRADING_DAYS_PER_YEAR: int = 251
    MAX_MONTHS_FORWARD: int = 13
    MIN_DAYS_TO_EXPIRY: int = 0
    MIN_VOLUME: float = 0
    
    # Path settings
    BASE_PATH: str = None
    
    # Which spreads to calculate
    CALCULATE_DOLLAR_SPREADS: bool = True
    CALCULATE_PERCENT_SPREADS: bool = True
    CALCULATE_ANNUAL_SPREADS: bool = True
    
    def __post_init__(self):
        if self.BASE_PATH is None:
            self.BASE_PATH = "."  # Current directory
            
    @property
    def PROCESSED_DATA_PATH(self) -> str:
        return f"{self.BASE_PATH}/processed_data"
        
    @property
    def RAW_DATA_PATH(self) -> str:
        return f"{self.BASE_PATH}/raw_data"