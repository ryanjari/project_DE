from sqlalchemy import create_engine
import pandas as pd
import scipy.stats as stats

# Informasi koneksi ke database PostgreSQL
db_user = 'airflow'
db_password = 'airflow'
db_host = 'localhost'  # Sesuaikan dengan host PostgreSQL Anda
db_port = '5431'  # Sesuaikan dengan port PostgreSQL Anda
db_name = 'airflow'

# Buat string koneksi SQLAlchemy
connection_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Buat koneksi menggunakan create_engine
engine = create_engine(connection_str)

# Lakukan kueri SQL untuk mengambil data dari tabel 'insurance'
query = "SELECT * FROM insurance"
result = engine.execute(query)

# Simpan hasil kueri dalam DataFrame
df = pd.DataFrame(result.fetchall(), columns=result.keys())

# Tutup koneksi database
engine.dispose()

#hipotesis
# H0 : Smokers do not pay more for insurance than non-smokers
# H1 : Smokers pay more for insurance than non-smokers
smoke = df[df['smoker'] == 'yes']
no_smoke =df[df['smoker'] == 'no']

# Perform t-test
t_stat, p_value = stats.ttest_ind(smoke["charges"], no_smoke["charges"])

if p_value < 0.05:
    print('tolak H0')
else: 
    print('terima H1')