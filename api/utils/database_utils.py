from sqlalchemy import inspect, text
import pandas as pd

def get_last_date(engine, table_name):
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return None
    
    query = f"SELECT MAX(DT_EXECUCAO) as last_date FROM \"{table_name}\""
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    return result[0] if result and result[0] else None

def update_database(engine, df, table_name):
    df.to_sql(table_name, engine, if_exists='append', index=False)

def adjust_table_schema(engine, df, table_name):
    df.head(0).to_sql(table_name, engine, if_exists='replace', index=False) 