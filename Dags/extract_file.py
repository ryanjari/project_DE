import pandas as pd

def read_google_sheets(sheet_id):
    # Construct the URL to the Google Sheets document
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv'
    
    # Read the data from the URL into a pandas DataFrame
    df = pd.read_csv(url)
    
    return df

def definisi_data():
     # Define your sheet_id
    sheet_id = '1vJ4eCptkQ6oYi0s3GOSVevTyLP9oPXBDU6j-m9Bnj4E'

    # Call the function to read the Google Sheets document
    resulting_dataframe = read_google_sheets(sheet_id)
    return resulting_dataframe
print(definisi_data())
