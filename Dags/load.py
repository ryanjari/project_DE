from transform import *
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


def insert_data():
    data = transformasi_data()
    data_df = pd.read_csv(data)
    table_name = 'insurance'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'localhost'  # Sesuaikan dengan host PostgreSQL Anda
    db_port = '5432'  # Sesuaikan dengan port PostgreSQL Anda
    db_name = 'airflow'

    # Buat string koneksi SQLAlchemy
    connection_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Buat koneksi menggunakan create_engine
    engine = create_engine(connection_str)

    data_df.to_sql(table_name, con=engine, if_exists='append', index=True)
    
