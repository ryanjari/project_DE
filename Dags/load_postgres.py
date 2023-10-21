from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from transform import transformasi_data
from sqlalchemy import create_engine
# Buat DAG
dag = DAG(
    'load_data_to_postgres',
    start_date=datetime(2023, 10, 20),
    schedule_interval=None,
)

def transform_and_load_data():
    transformed_data = transformasi_data()# Panggil fungsi transformasi_data() untuk mengubah data
    table_name = 'insurance'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
  # Sesuaikan dengan host PostgreSQL Anda
    db_port = '5432'  # Sesuaikan dengan port PostgreSQL Anda
    db_name = 'airflow'

    # Buat string koneksi SQLAlchemy
    connection_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Buat koneksi menggunakan create_engine
    engine = create_engine(connection_str)
    connection = engine.connect()  # Gantilah 'engine' dengan koneksi database yang sesuai
    transformed_data.to_sql('insurance', connection, if_exists='replace', index=False)  # Gantilah 'insurance' dengan nama tabel yang sesuai
    connection.close()

with dag:
    # Create the "insurance" table before loading data
    create_table_task = PostgresOperator(
        task_id='create_insurance_table',
        sql="""DROP TABLE IF EXISTS insurance;
        CREATE TABLE  insurance (
            -- Define other columns as needed
            age INT,
            sex VARCHAR,
            bmi INT,
            children INT,
            smoker VARCHAR,
            region VARCHAR,
            charges INT,
            age_group VARCHAR,
            bmi_group VARCHAR
        )
        """,
        postgres_conn_id='dibimbing_postgres',
        database='airflow',
    )

    # Transform and load data into the "insurance" table
    transform_and_load_task = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_and_load_data,
    )

# Menentukan urutan eksekusi task
create_table_task >> transform_and_load_task
