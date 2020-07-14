from __future__ import print_function

import time
import gspread
import pandas as pd
import numpy as np
import requests

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
gc = gspread.authorize(CREDENTIALS)

# Marketo Auth
mkto = Variable.get("creds_mkto", deserialize_json=True)
mc = MarketoClient(
    mkto["MKTO_CLIENT_MUNCHKIN"],
    mkto["MKTO_CLIENT_ID"], 
    mkto["MKTO_CLIENT_SECRET"],
)

# ON24 Auth
web = Variable.get("creds_on24", deserialize_json=True)

# Json reference pulls
j = "event_"
son = "sto20"
ref = Variable.get(j + son, deserialize_json=True)
ref_doc_url = ref["REF_DOC"]  # Url for reference doc
ref_doc_sheet = ref["REF_DOC_SHEET"]  # sheet inside reference doc
ref_cols = ref["REF_DOC_COLS"]
ref_file_id = ref["EVENT_FINAL_DOC"]
ref_file_summ = ref["EVENT_FINAL_SUMM"]
ref_file_sess = ref["EVENT_FINAL_SESS"]
ref_file_reg = ref["EVENT_FINAL_REG"]
ref_file_un = ref["EVENT_FINAL_UN_REG"]
file_web_reg = ref["EVENT_FINAL_ON24_REG"]
file_web_dl = ref["EVENT_FINAL_ON24_DL"]
reg_doc = ref["REGION_DOC"]
reg_doc_cols = ref["REGION_DOC_COLS"]
rt1 = ref["REF_RT"]
one_to_one = ref["REF_1TO1"]

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
    "freeText6",
    "formStringField6",
    "freeText7",
    "formStringField7",
    "formStringField8",
]

dice_cols = [
    "Program", 
    "ON24 ID",
    "Day", 
    "Session", 
    "Session Date", 
    "Theme", 
    "Speaker/Sponsor",
]

final_cols = [
    "Reg Month",
    "Reg Date",
    "First Name",
    "Last Name",
    "Company",
    "Job Title",
    "Email",
    "Phone Number",
    "Country",
    "US State",
    "Region",
    "Seniority",
    "User Category",
    "User Sub-Category",
    "Roundtable",
    "One-to-One",
    "Status",  
    "Technology Areas",
    "Edge Use Cases",
    "Project Currently Worked On",
]

for i in dice_cols:
    if i in ref_cols:
        final_cols.append(i)

rep_date = pd.to_datetime(date.today()).strftime("%F")
rep_mon = pd.to_datetime(date.today()).strftime("%B")


def make_list(lumns, sheet_ref):
    df_1 = pd.DataFrame(
        sheet_ref.col_values((sheet_ref.find(lumns[0])).col)[1:], columns=[lumns[0]]
    )

    for j in lumns[1:]:
        df_i = pd.DataFrame(
            sheet_ref.col_values((sheet_ref.find(j)).col)[1:], columns=[j]
        )
        df_1 = pd.concat([df_1, df_i], axis=1, sort=False)

    df_1.replace(np.nan,'',inplace=True)

    return df_1

def make_final_list(**context):
    ref_sheet = gc.open_by_key(ref_doc_url).worksheet(ref_doc_sheet)
    reg_sheet = gc.open_by_key(reg_doc).worksheet("Country List with Region")

    df_3 = make_list(ref_cols, ref_sheet)
    region_df = make_list(reg_doc_cols, reg_sheet)

    df_4 = df_3.loc[df_3["Program ID"] != ""].reset_index()
    
    df_4["Program"] = df_4["Program"]

    context["task_instance"].xcom_push(key="wp_ref", value=df_4)
    context["task_instance"].xcom_push(key="region_ref", value=region_df)

def member_check(mem_df, field_headers):
    if "progressionStatus" in mem_df.columns:
        att = mem_df.loc[mem_df["progressionStatus"] == ("Attended")]
        att_on = mem_df.loc[mem_df["progressionStatus"] == ("Attended On-demand")]
        ns = mem_df.loc[mem_df["progressionStatus"] == ("No Show")]
        reg = mem_df.loc[mem_df["progressionStatus"] == ("Registered")]

        mem_df = att.append(att_on, ignore_index=True)
        mem_df = mem_df.append(ns, ignore_index=True)
        mem_df = mem_df.append(reg, ignore_index=True)

        mem_df_1 = mem_df[
            field_headers + ["progressionStatus","membershipDate"]
            ]
    else:
        mem_df_1 = mem_df[[field_headers]]

    return mem_df_1

def export_marketo_program(program_id, field_headers):
    act1 = mc.execute(
        method="get_multiple_leads_by_program_id",
        programId=program_id,
        fields=field_headers,
        batchSize=None,
    )
    
    act2 = pd.DataFrame(
        act1,
        columns = field_headers + ["membership"],
    )

    act2_drop = act2.drop(["membership"], axis=1)

    act2_series = act2["membership"].apply(pd.Series)

    act3 = pd.concat([act2_drop, act2_series], axis=1)

    return act3

def make_unique_df(full_df, out, sub, short):
    out = pd.concat([out,full_df],axis=0)
    out.drop_duplicates(subset=sub, keep="first", inplace=True)
    out = pd.merge(out, short, on=sub, how="left")
    return out

def transform_cols(df1, dicto, idex):
    df1["Technology Areas"] = df1["formStringField7"]
    df1["Edge Use Cases"] = df1["formStringField8"]
    df1["Project Currently Worked On"] = df1["freeText7"]
    df1["Program"] = dicto["Program"][idex][13:]
    df1["ON24 ID"] = dicto["ON24 ID"][idex]
    df1["Day"] = dicto["Day"][idex]
    df1["Session"] = dicto["SC Titles"][idex]
    df1["Session Date"] = pd.to_datetime(dicto["Session Date"][idex]).strftime("%d %b %Y")
    df1["Theme"] = dicto["Theme"][idex]
    df1["Speaker/Sponsor"] = dicto["Speaker/Sponsor"][idex]

    df1.insert(
        0,
        "Reg Date",
        pd.to_datetime(df1["membershipDate"].str[:10]).dt.strftime("%d %b %Y"),
    )
    df1.insert(0, "Reg Month", pd.to_datetime(df1["Reg Date"]).dt.strftime("%B"))
    df1.insert(df1.columns.get_loc("state") + 1, "Region", "")
    df1["User_Sub_Category__c"] = df1["User_Sub_Category__c"].str.split('- ').str[1]

    df1.drop([
        "membershipDate",
        "formStringField7",
        "formStringField8",
        "freeText7"
        ],axis=1, inplace=True)
    df1.replace(np.nan,"",inplace=True)
    df1.columns = final_cols
    for i,j in enumerate(df1["First Name"]):
        if j == "" or df1["Last Name"][i] == "":
            continue
        df1["First Name"][i] = df1["First Name"][i][0].upper() + df1["First Name"][i][1:].lower()
        df1["Last Name"][i] = df1["Last Name"][i][0].upper() + df1["Last Name"][i][1:].lower()

def export_webcasts(client, params, web_id, cols):
    response = requests.get(
        "https://api.on24.com/v2/client/"
        + str(client)
        + "/event/"
        + str(web_id)
        + "/registrant",
        headers=params,
    )
    reg_list = response.json()["registrants"]
    df2 = pd.DataFrame.from_dict(reg_list)
    df2.replace(np.nan, "", inplace=True)
    web_cols = [
            "eventuserid",
            "firstname",
            "lastname",
            "company",
            "jobtitle",
            "email",
            "country",
            "workphone",
            "createtimestamp",
            "eventid",
        ]
    df2[["eventid"]] = web_id
    df3 = df2[[x for x in web_cols if x in df2.columns]]
    df3["Email"] = df3["email"]
    response = requests.get("https://api.on24.com/v2/client/"+str(client)+"/event/"+str(web_id)+"/attendee",headers=params)
    res_list = response.json()["attendees"]
    reshaped = []

    for user in res_list:
        user_added = False
        if "resources" in user:
            for resource in user["resources"]:
                reshaped.append({**user, **resource})

        if "questions" in user:
            for question in user["questions"]:
                if "answer" in question:
                    question["answer"]["response"] = question["answer"].pop("content")
                    question["answer"]["answertimestamp"] = question["answer"].pop(
                        "createtimestamp"
                    )
                    reshaped.append({**user, **question, **question["answer"]})
                else:
                    reshaped.append({**user, **question})

        if "polls" in user:
            for poll in user["polls"]:
                if "pollanswersdetail" in poll:
                    for polla in poll["pollanswersdetail"]:
                        polla["pollanswer"] = polla.pop("answer")
                        reshaped.append({**user, **poll, **polla})
                else:
                    reshaped.append({**user, **poll})

        if "surveys" in user:
            for survey in user["surveys"]:
                reshaped.append({**user, **survey})

        if "twitterwidget" in user:
            for tweet in user["twitterwidget"]:
                reshaped.append({**user, **tweet})

    df = pd.DataFrame.from_dict(reshaped)
    df4 = df3
    res4 = pd.DataFrame()
    if df.shape[0] > 0:
        drop_cols = [
            "resources",
            "questions",
            "answer",
            "twitterwidget",
            "polls",
            "surveys",
            "pollanswersdetail",
            "pollanswers",
        ]
        for j in drop_cols:
            if j in df.columns:
                df = df.drop([j], axis=1)
        df.replace(np.nan, "", inplace=True)

        res1 = df.drop(
            ["userstatus", "isblocked", "userprofileurl"], axis=1
        )

        df1 = res1[res1.columns[:16]]

        res2 = res1
        res2.insert(1, "Total Engagements", 0)

        engs = [
            "resourcesdownloaded",
            "askedquestions",
            "answeredpolls",
            "answeredsurveys",
        ]

        for e in engs:
            if e in res2.columns:
                res2["Total Engagements"] = res2["Total Engagements"] + res2[e]

        res3 = res2.loc[res2["Total Engagements"] > 0].drop(
            ["Total Engagements"], axis=1
        )
        dl = ["email", "sourceeventid"]
        res = ["resourceid", "resourceviewed", "resourceviewedtimestamp"]
        que = ["questionid", "createtimestamp", "content", "presentername"]
        ans = ["response", "answertimestamp"]
        pol = [
            "pollid",
            "pollsubmittedtimestamp",
            "pollquestionid",
            "pollquestion",
            "answercode",
            "pollanswer",
        ]
        twit = ["date", "tweetdescription"]
        if "resourceid" in res3.columns:
            dl = dl + res
        if "questionid" in res3.columns:
            dl = dl + que
        if "response" in res3.columns:
            dl = dl + ans
        if "pollid" in res3.columns:
            dl = dl + pol
        if "tweetdescription" in res3.columns:
            dl = dl + twit

        for j in dl:
            if j not in res3.columns:
                dl.remove(j)
        res4 = res3[dl]

        df4 = pd.merge(
            df3,
            df1.drop_duplicates(subset="email", keep="first"),
            on="eventuserid",
            how="left",
        ).drop(["cumulativeliveminutes","cumulativearchiveminutes"],axis=1)
        df4["Total Mins"] = df4["liveminutes"] + df4["archiveminutes"]
    
    response = requests.get("https://api.on24.com/v2/client/"+str(client)+"/event/"+str(web_id)+"/attendeesession",headers=params)
    avs_df = pd.DataFrame.from_dict(response.json()['attendeesession'])
    avs_df["duration"] = pd.to_timedelta(avs_df["duration"].str.replace("-",""),unit="s")

    avs_grp = avs_df.groupby(["id"],as_index=False)[["duration"]].sum()
    avs_grp["duration"] = avs_grp["duration"].astype(str)
    avs_grp.columns = ["eventuserid","duration"]
    
    df4 = pd.merge(df4,avs_grp,on="eventuserid",how="left")

    return df4, res4

def start_export(**context):
    wp_dict = context["task_instance"].xcom_pull(
        task_ids="make_final_list", key="wp_ref"
    )
    reg_dice = context["task_instance"].xcom_pull(
        task_ids="make_final_list", key="region_ref"
    )
    id_col = wp_dict["Program ID"]
    full = pd.DataFrame([])
    web_full = pd.DataFrame([])
    reg_full = pd.DataFrame([])
    res_full = pd.DataFrame([])
    un_email = pd.DataFrame([], columns=final_cols)
    sc_export = pd.DataFrame([])
    for x, y in enumerate(id_col):
        df_1 = export_marketo_program(y, field_names)
        web_reg, web_res = export_webcasts(
            web["WEB_CLIENT_ID"], web["WEB_PARAMS"], wp_dict["ON24 ID"][x], []
        )
        df_2 = member_check(df_1, field_names)
        transform_cols(df_2, wp_dict, x)
        df_short = df_2[["Email", "Status"]]
        df_short2 = df_2[["Email", "Session"]]
        df_short.columns = ["Email", wp_dict["Program"][x]]
        df_short2.columns = ["Email", wp_dict["Program"][x]]
        df_2["Region"] = pd.merge(
            df_2, reg_dice[["Country", "Operational Region"]], on="Country", how="left"
        )["Operational Region"]
        df_3 = df_2
        print(web_reg.columns)
        web1 = pd.merge(df_3, web_reg, on="Email", how="left")
        
        full = pd.concat([full, df_3], axis=0)
        web_full = pd.concat([web_full, web1], axis=0)
        reg_full = pd.concat([reg_full,web_reg],axis=0)
        res_full = pd.concat([res_full,web_res],axis=0)

        un_email = make_unique_df(full, un_email, "Email", df_short)
        sc_export = make_unique_df(full, sc_export, "Email", df_short2)
        print(wp_dict["Program"][x])
    
    un_email.drop(
        ["Status"] + dice_cols,
        axis=1,
        inplace=True,
    )
    sc_export.drop(
        ["Status"] + dice_cols,
        axis=1,
        inplace=True,
    )
    sc_export.replace(np.nan,'',inplace=True)
    sc_export["Programs"] = sc_export[wp_dict["Program"][0]]
    prog = list(wp_dict["Program"])
    for x,y in enumerate(sc_export["Email"]):
        for i,j in enumerate(wp_dict["Program"][1:]):
            if ';' + sc_export[wp_dict["Program"][i]][x] != ";":
                sc_export["Programs"][x] = sc_export["Programs"][x] + ';' + sc_export[wp_dict["Program"][i]][x]
        col_val = sc_export["Programs"][x]
        sc_export["Programs"][x]  = sc_export["Programs"][x][col_val.find(';')+1:]

    sc_export.drop(prog,axis=1,inplace=True)
    gc.open_by_key(ref_file_id).values_clear("SwapCard" + "!1:50000")
    export_data_to_sheets(sc_export, ref_file_id, "SwapCard", "A1", False)
    
    context["task_instance"].xcom_push(key="wp_ref", value=wp_dict)
    context["task_instance"].xcom_push(key="full_df", value=full)
    context["task_instance"].xcom_push(key="web_df", value=web_full)
    context["task_instance"].xcom_push(key="web_reg_df", value=reg_full)
    context["task_instance"].xcom_push(key="web_res_df", value=res_full)
    context["task_instance"].xcom_push(key="unique_df", value=un_email)
    
def export_roundtable(**context):
    wp_sub = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="wp_ref"
    )
    un_email = context["task_instance"].xcom_pull(task_ids="export_programs", key="unique_df")
    
    table = un_email.loc[un_email["Roundtable"] != ''].drop(["One-to-One"],axis=1)
    
    for j in rt1:
        sub = table[table['Roundtable'].str.contains(j)][["Email"]]
        sub[j] = "Registered"
        table = pd.merge(table,sub,on="Email",how="left")
    
    rt = table.drop([i for i in list(wp_sub["Program"])],axis=1)#[["First Name","Last Name","Company","Email","Status","Roundtable"]]
    export_data_to_sheets(rt, ref_file_id, "Roundtable", "A1", False)
    
def export_one_to_one(**context):
    wp_sub = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="wp_ref"
    )
    un_email = context["task_instance"].xcom_pull(task_ids="export_programs", key="unique_df")
    
    one = un_email.loc[un_email["One-to-One"] != ''].drop(["Roundtable"],axis=1)
    
    for k in one_to_one:
        sub = one[one['One-to-One'].str.contains(k)][["Email"]]
        sub[k] = "Registered"
        one = pd.merge(one,sub,on="Email",how="left")
    
    ot1 = one.drop([i for i in list(wp_sub["Program"])],axis=1)#[["First Name","Last Name","Company","Email","Status","One-to-One"]]
    export_data_to_sheets(ot1, ref_file_id, "1-to-1", "A1", False)

def export_sessions(**context):
    wpdict = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="wp_ref"
    )
    full = context["task_instance"].xcom_pull(task_ids="export_programs", key="full_df")
    web_full = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="web_df"
    )
    reg_full = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="web_reg_df"
    )
    res_full = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="web_res_df"
    )
    
    gc.open_by_key(ref_file_id).values_clear("MK+ON24" + "!2:50000")
    gc.open_by_key(ref_file_id).values_clear(file_web_dl + "!2:50000")
    gc.open_by_key(ref_file_id).values_clear(file_web_reg + "!2:50000")
    gc.open_by_key(ref_file_id).values_clear(ref_file_un + "!2:50000")
    gc.open_by_key(ref_file_id).values_clear(ref_file_reg + "!2:50000")
    for i, j in enumerate(wpdict["ON24 ID"]):
        if j == 0:
            continue
        sub = web_full.loc[web_full["eventid"] == j]
        sub1 = full.loc[full["ON24 ID"] == j]
        sub_cols = list(sub.columns[:51])
        print(reg_full.columns)
        sub_reg = reg_full.loc[reg_full["eventid"] == j][list(reg_full.columns)[:25]]
        print(sub_reg.columns)
    
        export_data_to_sheets(sub[sub_cols], ref_file_id, "MK+ON24", "A1", True)
        export_data_to_sheets(sub1,ref_file_id, ref_file_reg, "A1", True)
        export_data_to_sheets(sub_reg, ref_file_id, file_web_reg, "A1", True)
    
    export_data_to_sheets(res_full, ref_file_id, file_web_dl, "A1", True)
    
def export_summary(**context):
    wp_sub = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="wp_ref"
    )
    full = context["task_instance"].xcom_pull(task_ids="export_programs", key="full_df")
    web_full = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="web_df"
    )
    reg_full = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="web_reg_df"
    )
    un_email = context["task_instance"].xcom_pull(
        task_ids="export_programs", key="unique_df"
    )
    
    full["ON24 ID"] = pd.to_numeric(full["ON24 ID"])
    grp_sess = full.groupby(
        dice_cols, as_index=False
    )[["Email"]].count()
    grp_sess.columns = dice_cols + ["Total"]
    stats = list(full.Status.unique())
    reg = pd.crosstab(
        index=full["Program"],
        columns=full["Status"],
        values=full["Status"],
        aggfunc="count",
    ).fillna(0.0)
    grp_sess = pd.merge(grp_sess, reg, on="Program",how="left")
    print(grp_sess.columns)

    grp_date = un_email.groupby(["Reg Date"], as_index=False)[["Email"]].count()
    grp_date.columns = ["Reg Date", "Unique Registrations"]

    if "Day" in full.columns:
        grp_day = full.groupby(["Session Date", "Day"], as_index=False)[["Email"]].count()
        grp_day.columns = ["Session Date", "Day", "Registrations"]
        export_data_to_sheets(grp_day, ref_file_id, ref_file_summ, "D3", False)

    un_email["User Category"].replace(np.nan, "", inplace=True)
    grp_userc = un_email.groupby(["User Category"], as_index=False)[["Email"]].count()
    grp_userc.columns = ["User Category", "Unique Registrations"]

    grp_comp = un_email.groupby(["Company"], as_index=False)[["Email"]].count()
    grp_comp.columns = ["Company", "Unique Registrations"]

    export_data_to_sheets(un_email, ref_file_id, ref_file_un, "A1", False)
    export_data_to_sheets(grp_date, ref_file_id, ref_file_summ, "A10", False)
    export_data_to_sheets(grp_sess, ref_file_id, ref_file_sess, "A1", False)
    export_data_to_sheets(grp_userc, ref_file_id, ref_file_summ, "A3", False)
    export_data_to_sheets(grp_comp, ref_file_id, ref_file_summ, "D10", False)

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
    dag_id="event_dag",
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
    task_id="export_sessions",
    python_callable=export_sessions,
    provide_context=True,
    dag=dag,
)

task4 = PythonOperator(
    task_id="export_summary",
    python_callable=export_summary,
    provide_context=True,
    dag=dag,
)

task5 = PythonOperator(
    task_id="export_roundtable",
    python_callable=export_roundtable,
    provide_context=True,
    dag=dag,
)

task6 = PythonOperator(
    task_id="export_one_to_one",
    python_callable=export_one_to_one,
    provide_context=True,
    dag=dag,
)

task1 >> task2 >> [task3, task4, task5, task6]