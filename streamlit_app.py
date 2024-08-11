import altair as alt
import duckdb
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import os.path
import json

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from io import BytesIO

st.set_page_config(page_title="Sabadell Transactions", page_icon="🤑")
st.title("💰 Sabadell Transactions")

def generate_credentials_json():
    credentials_file_name = 'credentials.json'
    credentials = {
       "web": {
          "client_id": st.secrets["client_id"],
          "project_id": "duckdbhome",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_secret": st.secrets["client_secret"],
       }
    } 
    with open('credentials.json', 'w') as json_file:
        json.dump(credentials, json_file)
    return f"{credentials_file_name}"

def get_service():
  creds = None
  service = None
  SCOPES = ['https://www.googleapis.com/auth/drive']
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          generate_credentials_json(), SCOPES
      )
      flow.authorization_url(
        access_type='offline',
        prompt='consent')
      
      creds = flow.run_local_server(port=0)
    #   creds = flow.run_local_server(port=56104)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())
  
  service = build('drive', 'v3', credentials=creds)
  return service
  
@st.cache_data
def download_file(_service, file_id, file_name):
    request = _service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fd=fh, request=request)
    done = False

    while not done:
        status, done = downloader.next_chunk()
        print("Download Progress: {0}".format(int(status.progress() * 100))) 
        fh.seek(0)

    with open(file_name, 'wb') as f:
        f.write(fh.read())
        f.close()

@st.cache_data
def get_files_in_folder(_service, folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = _service.files().list(q=query, fields="files(id, name, createdTime, modifiedTime)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(f"{item['name']} ({item['id']}) - {item['createdTime']}/{item['modifiedTime']}")
    return items

if 'con' not in st.session_state:
    service = get_service()

    folder_id = st.secrets["folder_id"]
    files = get_files_in_folder(service, folder_id)
    files_df = pd.DataFrame(files, columns=['name', 'id', 'createdTime', 'modifiedTime']) 
    files_df = files_df.sort_values('modifiedTime', ascending=False)
    file_id = files_df.iloc[0]['id']
    file_name = 'db.duckdb'
    download_file(service, file_id, file_name)

    st.session_state.con = duckdb.connect(database=f"{file_name}", read_only=True)

con=st.session_state.con

@st.cache_data
def load_data():
    df = con.execute(
    '''
        select 
            fecha_operativa, 
            datetrunc('month', fecha_operativa) as mes, 
            extract('year' from fecha_operativa) as año, 
            concepto, 
            saldo,
            if(importe > 0, 'Ingreso', 'Gasto') as tipo,
            importe 
        from raw.sabadell_transactions
    ''').df()
    return df


def net_balance_per_month_graph(df):
    st.subheader('Saldo Neto por mes')
    df_monthly = df.groupby('mes')['importe'].sum().reset_index()
    st.line_chart(df_monthly, x='mes')
   
def net_balance_per_month_filtered(df):
    transaction_types = st.multiselect(
        "Tipo",
        df.tipo.unique(),
        ["Ingreso", "Gasto"],
    )

    years = st.slider("Years", 2013, 2024, (2013, 2024))

    df_filtered = df[(df["tipo"].isin(transaction_types)) & (df["año"].between(years[0], years[1]))]
    df_reshaped = df_filtered.pivot_table(
        index="año", columns="tipo", values="importe", aggfunc="sum", fill_value=0
    )
    if 'Ingreso' in df_reshaped.columns and 'Gasto' in df_reshaped.columns:
        df_reshaped['neto'] = df_reshaped['Gasto'] + df_reshaped['Ingreso']
    df_reshaped = df_reshaped.sort_values(by="año", ascending=False)

    st.dataframe(
        df_reshaped,
        use_container_width=True,
        column_config={"año": st.column_config.TextColumn("Año")},
    )
   
    df_chart = pd.melt(
        df_reshaped.reset_index(), id_vars="año", var_name="tipo", value_name="importe"
    )
    chart = (
        alt.Chart(df_chart)
        .mark_line()
        .encode(
            x=alt.X("año:N", title="Año"),
            y=alt.Y("importe:Q", title="Total ($)"),
            color="tipo:N",
        )
        .properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)

df = load_data()
net_balance_per_month_graph(df)
net_balance_per_month_filtered(df)

