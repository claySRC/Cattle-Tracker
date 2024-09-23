import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import requests
from io import StringIO

# Function to fetch CSV from GitHub
def fetch_csv_from_github(url):
    response = requests.get(url)
    if url == 'https://github.com/claySRC/Cattle-Tracker/edit/main/src/Bancroft%20mown%20v1_0.c':
        df = pd.read_csv(StringIO(response.text), skiprows=25)
    elif url == 'https://raw.githubusercontent.com/claySRC/Cattle-Tracker/refs/heads/main/src/M_srad.csv':
        df = pd.read_csv(StringIO(response.text), parse_dates=[['date', 'time']])
    elif url == 'https://raw.githubusercontent.com/claySRC/Cattle-Tracker/refs/heads/main/src/precip.csv':
        df = pd.read_csv(StringIO(response.text), parse_dates=[['date', 'time']])
    else:
        df = pd.read_csv(StringIO(response.text))
    return df

# GitHub raw URLs for the four CSV files
csv_urls = {
    'bancroft_mown': 'https://raw.githubusercontent.com/claySRC/Cattle-Tracker/refs/heads/main/src/Bancroft%20mown%20v1_0.csv',
    'm_srad': 'https://raw.githubusercontent.com/claySRC/Cattle-Tracker/refs/heads/main/src/M_srad.csv',
    'precip': 'https://raw.githubusercontent.com/claySRC/Cattle-Tracker/refs/heads/main/src/precip.csv',
    'treatment': 'https://raw.githubusercontent.com/claySRC/Cattle-Tracker/refs/heads/main/src/treatment_avg_sm_temp_15min.csv'
}

# Processing function for Bancroft mown CSV
def process_bancroft_mown(df):
    df = df.iloc[1:].reset_index(drop=True)
    df['datetime'] = pd.to_datetime(df['ENDING_DATETIME'])
    df = df.drop(columns=['TIMESTAMP', 'STARTING_DATETIME', 'MIDPOINT_DATETIME', 'ENDING_DATETIME'], errors='ignore')
    df.iloc[:, :-1] = df.iloc[:, :-1].apply(pd.to_numeric, errors='coerce')
    df = df.set_index('datetime').resample('H').mean()
    return df

# Processing function for M_srad CSV
def process_m_srad(df):
    df['datetime'] = pd.to_datetime(df['date_time'])
    df = df.set_index('datetime').resample('H').mean()
    df = df.drop(columns=['Unnamed: 0'], errors='ignore')
    df.columns = ['Irrad_' + col for col in df.columns]
    return df

# Processing function for Precip CSV
def process_precip(df):
    df['datetime'] = pd.to_datetime(df['date_time'])
    df = df.set_index('datetime').resample('H').mean()
    df = df.drop(columns=['Unnamed: 0'], errors='ignore')
    df.columns = ['Precip_' + col for col in df.columns]
    return df

# Processing function for Treatment CSV
def process_treatment(df):
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df = df.drop(['date', 'time'], axis=1)
    df_temp = df.pivot_table(index='datetime', columns=['treatment', 'zone'], values='temp')
    df_wc = df.pivot_table(index='datetime', columns=['treatment', 'zone'], values='WC')
    df_temp.columns = [f'temp_{t}_{z}' for t, z in df_temp.columns]
    df_wc.columns = [f'WC_{t}_{z}' for t, z in df_wc.columns]
    df_combined = pd.concat([df_temp, df_wc], axis=1)
    df_combined = df_combined.resample('H').mean()
    return df_combined

# Dash app setup
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Download Combined CSV"),
    html.Button("Download", id="download-button", n_clicks=0),
    dcc.Download(id="download-csv"),
])

@app.callback(
    Output("download-csv", "data"),
    Input("download-button", "n_clicks"),
    prevent_initial_call=True,
)
def combine_and_download(n_clicks):
    # Fetch each CSV from GitHub
    bancroft_df = fetch_csv_from_github(csv_urls['bancroft_mown'])
    m_srad_df = fetch_csv_from_github(csv_urls['m_srad'])
    precip_df = fetch_csv_from_github(csv_urls['precip'])
    treatment_df = fetch_csv_from_github(csv_urls['treatment'])

    # Process each CSV
    bancroft_df = process_bancroft_mown(bancroft_df)
    m_srad_df = process_m_srad(m_srad_df)
    precip_df = process_precip(precip_df)
    treatment_df = process_treatment(treatment_df)

    # Combine the dataframes
    combined_df = bancroft_df.join([m_srad_df, precip_df, treatment_df], how="outer")
    combined_df['Date'] = combined_df.index.date
    combined_df['Time'] = combined_df.index.time
    combined_df = combined_df[['Date', 'Time'] + [col for col in combined_df.columns if col not in ['Date', 'Time']]]

    # Provide the combined CSV for download
    return dcc.send_data_frame(combined_df.to_csv, "combined_data.csv")

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
