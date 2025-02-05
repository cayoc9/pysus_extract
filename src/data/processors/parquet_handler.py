import pyarrow.parquet as pq

class ParquetProcessor:
    def __init__(self, schema_validator):
        self.validator = schema_validator
    
    def process_files(self, file_paths):
        for path in file_paths:
            try:
                table = pq.read_table(path)
                if self.validator.validate_schema(table.schema):
                    yield table.to_pandas()
            except Exception as e:
                logger.error(f"Error processing {path}: {str(e)}") 