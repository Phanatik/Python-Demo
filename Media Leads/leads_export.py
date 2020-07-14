from __future__ import print_function

import time
import gspread
import pandas as pd
import numpy as np
from builtins import range
from pprint import pprint

# airflow
from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

from datetime import date, datetime
from marketorestpython.client import MarketoClient
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

# Google Auth
CREDS_FILE_PATH = Variable.get("creds_google", deserialize_json=True)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_dict(CREDS_FILE_PATH, SCOPES)
sheets_service = discovery.build(
    "sheets", "v4", credentials=CREDENTIALS, cache_discovery=False
)

# Marketo Auth
mkto = Variable.get("creds_mkto", deserialize_json=True)
mc = MarketoClient(
    mkto["MKTO_CLIENT_MUNCHKIN"], mkto["MKTO_CLIENT_ID"], mkto["MKTO_CLIENT_SECRET"]
)

ref = Variable.get("export_leads", deserialize_json=True)

ref_doc_url = ref["REF_DOC"]  # Url for reference doc
ref_doc_sheet = ref["REF_DOC_SHEET"]  # sheet inside reference doc
ref_cols = ref["REF_DOC_COLS"]

ref_file_id = ref["EVENT_FINAL_DOC"]
ref_file_summ = ref["EVENT_FINAL_SUMM"]
field_names = [
    "firstName",
    "lastName",
    "company",
    "title",
    "email",
    "phone",
    "country",
    "state",
    "seniority",
    "userCategory",
    "User_Sub_Category__c",
]

rep_date = pd.to_datetime(date.today()).strftime("%F")
rep_mon = pd.to_datetime(date.today()).strftime("%B")


def make_final_list(**context):
    gc = gspread.authorize(CREDENTIALS)
    url = ref_doc_url
    lumns = ref_cols
    sheet_ref = ref_doc_sheet

    ref_file = gc.open_by_key(url)
    ref_sheet = ref_file.worksheet(sheet_ref)

    df_1 = pd.DataFrame(
        ref_sheet.col_values((ref_sheet.find(lumns[0])).col)[1:], columns=[lumns[0]]
    )

    for j in lumns[1:]:
        df_i = pd.DataFrame(
            ref_sheet.col_values((ref_sheet.find(j)).col)[1:], columns=[j]
        )
        df_1 = pd.concat([df_1, df_i], axis=1, sort=False)

    df_1.insert(df_1.shape[1], rep_date, 0)
    df_1.replace(np.nan, "", inplace=True)
    
    df_2 = df_1.loc[df_1["Sponsors"] == "Sponsor1"].reset_index()
    df_2 = df_2.loc[df_2["Region"] == "EMEA/NAM"].reset_index()

    ref_spon = ref_file.worksheet("Ref")  # sponsor info

    spon_name = ref_spon.col_values((ref_spon.find("Sponsors")).col)
    spon_id = ref_spon.col_values((ref_spon.find("Id")).col)

    spon_ref = dict(zip(spon_name[1:], spon_id[1:]))

    context["task_instance"].xcom_push(key="wp_ref", value=df_2)
    context["task_instance"].xcom_push(key="sponsor_url", value=spon_ref)


def member_check(channel, df4, asset):
    if channel == "DCD Webinar-Debate":
        att = df4.loc[df4["progressionStatus"] == ("Attended")]
        att_on = df4.loc[df4["progressionStatus"] == ("Attended On-demand")]
        ns = df4.loc[df4["progressionStatus"] == ("No Show")]

        df5 = att.append(att_on, ignore_index=True)
        df5 = df5.append(ns, ignore_index=True)
    else:
        if asset == "Sponsor4":
            reg = df4.loc[df4["progressionStatus"] == ("Download - Pending Approval")]
            att = df4.loc[df4["progressionStatus"] == ("Download - Approved")]
            df5 = reg.append(att, ignore_index=True)
        else:
            df5 = df4.loc[df4["progressionStatus"] == ("Download - Approved")]
        # df5 = df4

    df5 = df5[
        [
            "firstName",
            "lastName",
            "company",
            "title",
            "email",
            "phone",
            "country",
            "state",
            "seniority",
            "userCategory",
            "User_Sub_Category__c",
            "progressionStatus",
            "membershipDate",
        ]
    ]

    return df5


def export_marketo_program(program_id, field_headers):
    act1 = mc.execute(
        method="get_multiple_leads_by_program_id",
        programId=program_id,
        fields=field_headers,
        batchSize=None,
    )

    act2 = pd.DataFrame(
        act1,
        columns=[
            "id",
            "firstName",
            "lastName",
            "company",
            "title",
            "email",
            "phone",
            "country",
            "state",
            "seniority",
            "userCategory",
            "User_Sub_Category__c",
            "membership",
        ],
    )

    act2_drop = act2.drop(["membership"], axis=1)

    act2_series = act2["membership"].apply(pd.Series)

    act3 = pd.concat([act2_drop, act2_series], axis=1)

    return act3


def transform_cols(df1, dicto, i):
    df1["Campaign"] = dicto["Sub-Campaign"][i]
    df1["Channel"] = dicto["Channel"][i]
    df1["Sponsors"] = dicto["Sponsors"][i]
    df1["Region"] = dicto["Region"][i]

    df1.insert(0, "Report Through Date", pd.to_datetime(df1["membershipDate"].str[:10]))
    df1.insert(0, "Report Delivery Date", rep_date)
    df1.insert(
        0,
        "Report Delivery Month",
        pd.to_datetime(df1["Report Delivery Date"]).dt.strftime("%B"),
    )
    df1.insert(
        df1.columns.get_loc("progressionStatus"), "Assets", dicto["Assets Name"][i]
    )

    # print(df1)

    df1.drop(["membershipDate"], axis=1, inplace=True)

    df1.columns = [
        "Report Delivery Month",
        "Report Delivery Date",
        "Report Through Date",
        "First Name",
        "Last Name",
        "Company",
        "Job Title",
        "Email",
        "Phone Number",
        "Country",
        "US State",
        "Seniority",
        "User Category",
        "User Sub-Category",
        "Assets",
        "Status",
        "Campaign",
        "Channel",
        "Sponsors",
        "Region",
    ]

    df1.replace("Download - Approved", "Downloaded", inplace=True)
    df1.replace("Download - Pending Approval", "Downloaded", inplace=True)
    df1.replace("DCD Webinar-Debate", "Webinar", inplace=True)

    if "Sponsor2" in dicto["Sponsors"][i]:
        df1.insert(df1.columns.get_loc("Region") - 1, "Marketo Program", "")
        df1.replace("Whitepaper", "Content Syndication", inplace=True)
    elif "Sponsor3" == dicto["Sponsors"][i]:
        df1.insert(df1.shape[1], "I agree", "Yes, I agree")
        df1.drop(["Channel","Region","Sub-Campaign"],axis=1,inplace=True)

def start_export(**context):
    wp_dict = context["task_instance"].xcom_pull(
        task_ids="make_final_list", key="wp_ref"
    )
    full = pd.DataFrame([])

    for x, y in enumerate(wp_dict["Program ID"]):

        asset_name = wp_dict["Assets Name"][x]
        file_name = wp_dict["Sponsors"][x]

        if y == "" or y == np.nan:
            continue

        sheet = export_marketo_program(y, field_names)

        if "progressionStatus" not in sheet.columns:
            continue

        sheet1 = member_check(wp_dict["Channel"][x], sheet, file_name)

        sheet2 = sheet1

        transform_cols(sheet2, wp_dict, x)

        if "Webinar" in wp_dict["Channel"][x]:
            sheet3 = sheet2.loc[
                sheet2["Report Through Date"]
                > (pd.to_datetime(rep_date) - np.timedelta64(7, "D"))
            ]
        else:
            sheet3 = sheet2

        leads = sheet3.shape[0]

        wp_dict.update(pd.DataFrame({rep_date: [leads]}, index=[x]))

        if leads == 0:
            continue

        full = pd.concat([full, sheet3], axis=0, sort=False)
        print(asset_name)
    # export_summary(wp_dict)
    context["task_instance"].xcom_push(key="wp_ref", value=wp_dict)
    context["task_instance"].xcom_push(key="full_df", value=full)
    # return full, wp_dict


def export_sponsor_lists(**context):
    wpdict = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="wp_ref"
    )
    full2 = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="full_df"
    )
    spon_ref = context["task_instance"].xcom_pull(
        task_ids="make_final_list", key="sponsor_url"
    )

    un_spon = wpdict.groupby(["Sponsors"], as_index=False)[[rep_date]].sum()

    for i, j in enumerate(un_spon["Sponsors"]):

        if un_spon[rep_date][i] == 0 or spon_ref[j] == "":
            continue

        full1 = full2

        sheet5 = full1.loc[full1["Sponsors"] == j].drop(["Sponsors"], axis=1)

        sheet5["Report Through Date"] = sheet5["Report Through Date"].dt.strftime("%F")

        export_data_to_sheets(sheet5,spon_ref[j],"Leads","A1",True)


def export_summary(**context):
    wp_sub = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="wp_ref"
    )

    grp_spon = wp_sub.groupby(["Sponsors", "Region"], as_index=False)[[rep_date]].sum()

    export_data_to_sheets(wp_sub, ref_file_id, ref_file_summ, "J1", False)
    export_data_to_sheets(grp_spon, ref_file_id, ref_file_summ, "A1", False)


def export_data_to_sheets(df, gsheetId, shName, cell, append):
    if df.shape[0] > 0:
        df.replace(np.nan, "", inplace=True)
        df1 = df.T.reset_index().T.values.tolist()
        if append:
            response_date = (
                sheets_service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=gsheetId,
                    valueInputOption="USER_ENTERED",
                    range=shName + "!" + cell,
                    body=dict(majorDimension="ROWS", values=df1[1:]),
                )
                .execute()
            )
        else:
            response_date = (
                sheets_service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=gsheetId,
                    valueInputOption="USER_ENTERED",
                    range=shName + "!" + cell,
                    body=dict(majorDimension="ROWS", values=df1),
                )
                .execute()
            )


args = {
    "owner": "Massum",
    "start_date": days_ago(2),
}
# https://crontab.guru/
dag = DAG(
    dag_id="leads_export",
    default_args=args,
    schedule_interval=None,  #'0 18 * * 3,4',
    tags=["example"],
)

task1 = PythonOperator(
    task_id="make_final_list",
    python_callable=make_final_list,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id="export_programs",
    python_callable=start_export,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id="make_sponsor_lists",
    python_callable=export_sponsor_lists,
    provide_context=True,
    dag=dag,
)

task4 = PythonOperator(
    task_id="export_summary",
    python_callable=export_summary,
    provide_context=True,
    dag=dag,
)

task1 >> task2 >> [task3, task4]
