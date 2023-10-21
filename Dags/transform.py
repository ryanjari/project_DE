import pandas as pd
import numpy as np
from extract_file import *
from sqlalchemy import create_engine, text

def transformasi_data():
    df = definisi_data()

    def konvert_int(df):
        # Ganti koma dengan titik dan ubah menjadi float
        df['bmi'] = df['bmi'].str.replace(',', '.').astype(float)

        # Mengkonversi ke integer
        df['bmi'] = df['bmi'].astype(int)
        return df

    def kategori_umur(df):
        df['age_group'] = np.where((df['age'] >= 12) & (df['age'] <= 25), 'remaja',
                                  np.where((df['age'] >= 25) & (df['age'] <= 45), 'dewasa',
                                           np.where(df['age'] >= 46, 'lansia', 'unknown')))
        return df

    def kategori_bmi(df):
        df['bmi_group'] = np.where(df['bmi'] < 17, 'sangat kurus',
                                   np.where((df['bmi'] >= 17) & (df['bmi'] <= 18), 'kurus',
                                            np.where((df['bmi'] >= 18.5) & (df['bmi'] < 24.9), 'normal',
                                                     np.where((df['bmi'] >= 25) & (df['bmi'] <= 29.9), 'gemuk berlebih',
                                                              np.where(df['bmi'] >= 30, 'obesitas', 'unknown')))))
        return df

    # Apply the data transformations
    df = konvert_int(df)
    df = kategori_umur(df)
    df = kategori_bmi(df)

    
    
    return df
# Call transformasi_data() to read and transform the Google Sheets data

