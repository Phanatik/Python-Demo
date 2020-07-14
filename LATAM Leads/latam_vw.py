import pandas as pd
import numpy as np
import gspread
import os
import time
import requests
import json

from datetime import date, datetime
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
start = datetime.now()
# Google Auth
CREDS_FILE_PATH = "credentials.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/bigquery.readonly"]
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE_PATH, SCOPES)
gc = gspread.authorize(CREDENTIALS)

sheets_service = discovery.build("sheets", "v4", credentials=CREDENTIALS)
client = bigquery.Client(project="bq_project")

def run_bq_query(path):
    query = open(path, "r").read()

    query_job = client.query(query)

    results = query_job.result()
    return results.to_dataframe()

def export_data_to_sheets(df, gsheetId, shName, cell):
    if df.shape[0] > 0:
        df.replace(np.nan, "", inplace=True)
        df1 = df.T.reset_index().T.values.tolist()           
        response_date = (
            sheets_service.spreadsheets().values().append(
                spreadsheetId=gsheetId,
                valueInputOption="USER_ENTERED",
                range=shName + "!" + cell,
                body=dict(majorDimension="ROWS", values=df1[1:]),
            ).execute()
        )
    print(shName)
    print(df.columns)

df = run_bq_query("query.txt")
df1 = run_bq_query("query1.txt")

sub_df = df[list(df.columns)[:-3]]
sub_df1 = df1[list(df1.columns)[:-3]]

if "createddate" in list(sub_df.columns):
    sub_df["createddate"] = sub_df["createddate"].astype(str)

if "createddate" in list(sub_df1.columns):
    sub_df1["createddate"] = sub_df1["createddate"].astype(str)
    
gs_id = "id"

g_sheet = gc.open_by_key(gs_id)
g_sheet.values_clear('Ref!3:50000')

export_data_to_sheets(sub_df, gs_id, "Ref", "A3")
export_data_to_sheets(sub_df1, gs_id, "Ref", "H3")
g_wsheet = g_sheet.duplicate_sheet(g_sheet.worksheet("Ref").id, insert_sheet_index=len(g_sheet.worksheets()), new_sheet_id=None, new_sheet_name=start.strftime("%d/%m"))

print(str(datetime.now()-start))
