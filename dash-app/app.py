#import all modul
import dash
from dash import dcc,html
from flask import Flask
import plotly.express as px 
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
import scipy.stats as stats


#initiate  the app
server = Flask(__name__)
app = dash.Dash(__name__,server=server,external_stylesheets = [dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP])

#read the files
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


#build the components
Header_component = html.H1('Insurance analysis Dashboard',style={'color':'darkcy'})

#visual components

# Iterate through the data and add bars with different colors
#componen1
smoker_counts = df['smoker'].value_counts().reset_index()
smoker_counts.columns = ['smoker', 'count']

# Create a figure
co = go.Figure()

# Define colors for 'yes' and 'no' categories
colors = {'yes': 'red', 'no': 'blue'}

# Create a pie chart
co.add_trace(go.Pie(
    labels=smoker_counts['smoker'],
    values=smoker_counts['count'],
    marker=dict(colors=[colors[category] for category in smoker_counts['smoker']]),
))

# Customize the layout
co.update_layout(
    title='Smoker Distribution',
)

#componen2
# Group the data by 'smoker' and 'sex' and count the occurrences
grouped_data = df.groupby(['sex', 'smoker']).size().reset_index(name='count')

# Create a figure
countfig = go.Figure()

# Define colors for smokers and non-smokers
colors = {'yes': 'blue', 'no': 'green'}

# Iterate through the data and add bars with different colors
for i, row in grouped_data.iterrows():
    countfig.add_trace(go.Bar(
        x=[row['sex']],
        y=[row['count']],
        text=[row['count']],
        textposition='auto',
        marker_color=colors[row['smoker']],
        name=row['smoker'],
    ))

# Customize the layout
countfig.update_layout(
    title='Count of Smokers and Non-Smokers by Gender',
    xaxis_title='Gender',
    yaxis_title='Count of Smokers',
)

#component3

fig = go.Figure()

# Customize the layout
fig.add_trace(go.Box(
    x= df['smoker'],
    y = df['charges']
))

fig.update_layout(
    xaxis_title='Smoker',
    yaxis_title='Total Charges',
)

fig.show()
#component 4
# Group the data by 'bmi_group' and 'smoker' and count the occurrences
e = df.groupby(['bmi_group', 'smoker']).size().unstack().reset_index()
e.columns = ['bmi_group', 'no', 'yes']

# Create a figure for the bar chart
cs = go.Figure()

# Define colors for 'yes' and 'no' categories
colors = {'yes': 'red', 'no': 'blue'}

# Create separate bars for 'yes' and 'no' smokers
for column in ['yes', 'no']:
    cs.add_trace(go.Bar(
        x=e['bmi_group'],
        y=e[column],
        name=f'Smokers ({column})',
        marker_color=colors[column],
    ))

# Customize the layout
cs.update_layout(
    title='Smoker Count by BMI Group',
    xaxis_title='BMI Group',
    yaxis_title='Count of Smokers',
)

#component 5
#groupby age_group berdasarkan smokers
w = df.groupby(['age_group', 'smoker']).size().unstack().reset_index()
w.columns = ['age_group', 'no', 'yes']

cd = go.Figure()

# Define colors for 'yes' and 'no' categories
colors = {'yes': 'red', 'no': 'blue'}

# Create a bar chart
for column in ['yes', 'no']:
    cd.add_trace(go.Bar(
        x=w['age_group'],
        y=w[column],
        name=column,
        marker_color=colors[column],
    ))

# Customize the layout
cd.update_layout(
    title='Smoker Count by age Group',
    xaxis_title='age Group',
    yaxis_title='Count smoker',
)



# Show the figure
#component 6
b = df.groupby('age_group').aggregate({'charges':'sum'})
b = b.reset_index()  # Menetapkan ulang indeks dan menyimpan perubahan ke objek 'b'

g = go.Figure()
g.add_trace(go.Bar(
    x=b['age_group'],
    y=b['charges'],
    marker=dict(color=['blue', 'green', 'red']),  # Atur warna dengan benar
    name='Charges by Age Group'
))

# Menyesuaikan tata letak
g.update_layout(
    title='Total Charges by Age Group',
    xaxis_title='Age Group',
    yaxis_title='Total Charge'
)

#component 7
f = df.groupby('region').aggregate({'charges':'sum'}).reset_index()

h =go.Figure()
h = go.Figure()
h.add_trace(go.Bar(
    x=f['region'],
    y=f['charges'],
    name='Charges by Region'
))

# Menyesuaikan tata letak
h.update_layout(
    title='Total Charges by Region',
    xaxis_title='Region',
    yaxis_title='Total Charge'
)
#Design the app layout
app.layout = html.Div(
    [
        dbc.Row([
            Header_component]),
        dbc.Row([dbc.Col(dcc.Graph(figure=co)
                        ),
                 dbc.Col(dcc.Graph(figure=countfig)),
                 dbc.Col(dcc.Graph(figure=cs)),
                 dbc.Col(dcc.Graph(figure=cd))]),
        dbc.Row([dbc.Col(dcc.Graph(figure=fig)),dbc.Col(dcc.Graph(figure=g)),dbc.Col(dcc.Graph(figure=h))])
        
    ]
)
#run the app
app.run_server(debug = True)