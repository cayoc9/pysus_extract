from pydantic import BaseModel, validator
import pandas as pd

class DataValidator:
    @staticmethod
    def validate_dtypes(df: pd.DataFrame, expected_dtypes: dict) -> bool:
        return all(str(df[col].dtype) == dtype for col, dtype in expected_dtypes.items())
    
    @classmethod
    def validate_date_format(cls, date_series, format='%Y%m%d'):
        try:
            pd.to_datetime(date_series, format=format)
            return True
        except ValueError:
            return False 