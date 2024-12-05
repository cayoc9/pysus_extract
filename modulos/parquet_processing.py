import os
import pyarrow.parquet as pq
import pandas as pd
import logging

def get_columns_from_files(parquet_files):
    all_columns = set()
    for file in parquet_files:
        if not os.path.isfile(file):
            logging.warning(f"Arquivo {file} não é um arquivo. Pulando.")
            continue
        try:
            parquet_file_obj = pq.ParquetFile(file)
            sample_batch = next(parquet_file_obj.iter_batches(batch_size=1))
            df_sample = sample_batch.to_pandas()
            all_columns.update(df_sample.columns)
        except Exception as e:
            logging.warning(f"Erro ao obter colunas do arquivo {file}: {e}")
    return list(all_columns)

def read_parquet_in_batches(parquet_file, batch_size=100000):
    try:
        parquet_file_obj = pq.ParquetFile(parquet_file)
        for batch in parquet_file_obj.iter_batches(batch_size=batch_size):
            df_chunk = batch.to_pandas()
            if df_chunk.empty:
                logging.warning(f"Batch vazio no arquivo {parquet_file}. Pulando.")
                continue
            yield df_chunk
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo Parquet {parquet_file}: {e}", exc_info=True)
        raise e
