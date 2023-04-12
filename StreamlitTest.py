from xmlrpc.client import DateTime
import streamlit as st
import snowflake.connector 
import pandas as pd
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from datetime import datetime

SNOWFLAKE_CONN_INFO = {
    "driver": "snowflake",
    "user": st.secrets.snowflake.user,
    "account": st.secrets.snowflake.account,
    "database": "FDR_DB",
    "warehouse": st.secrets.snowflake.warehouse,
    "role": st.secrets.snowflake.role,
    "schema": "FDR_SOURCE",
    "password": st.secrets.snowflake.password
}
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )


conn = init_connection()
results=pd.read_sql_query("select * from FDR_DB.FDR_SOURCE.DIGITAL",conn )
#results=pd.read_sql_query("Select * from FDR_DB.FDR_STAGE.STREAMLIT_TEST;",conn)
#st.dataframe(data=results)
#st.info(len(results))
st.set_page_config(
page_title="My dashboard",
layout="wide", 
initial_sidebar_state="expanded",
)

st.header("Table Contents")
gd=GridOptionsBuilder.from_dataframe(results)
gd.configure_side_bar()
gd.configure_default_column(editable=True,groupable=True,filterable=True,sortable=True)
gd.configure_columns(["PARENTORGKEY","CHILDORGKEY"],editable=False)
gd.configure_selection(selection_mode='multiple',use_checkbox=True)
gd.configure_pagination(enabled=True,paginationAutoPageSize=True, paginationPageSize=100)
#columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS
gridoptions=gd.build()

grid_table=AgGrid(results,gridOptions=gridoptions)
sel_row=grid_table["selected_rows"]
with st.form(key='Save'):
    submit_button = st.form_submit_button(label='Save Changes')

if submit_button: 
    for i in sel_row:
        now = datetime.now()
        date_time_str=now.strftime("%Y-%m-%d %H:%M:%S")
        sql_update="UPDATE FDR_DB.FDR_SOURCE.DIGITAL SET \
                                       BPN= %s,\
                                     PRIMARY_ID=%s,\
                                     SFDC_ACCOUNTID=%s,\
                                     OPPORTUNITY_ID=%s,\
                                     OMS_ID=%s,\
                                     OMS_PRODUCT_ORGANIZATION_ID=%s,\
                                     MERCHANT_TYPE=%s,\
                                     MERCH_ID=%s,\
                                     MERCHANTNUMBER=%s,\
                                     PARENTORG= %s,\
                                     OMS_PARENTORG=%s,\
                                     ORG_NAME=%s,\
                                     MASTERPARENTNAME=%s,\
                                     BRAND=%s,\
                                     CHANNELPARTNER=%s,\
                                     CSS_PRODUCT=%s,\
                                     CSS_PRODUCT_TYPE=%s,\
                                     CSS_MASTER_STATUS=%s,\
                                     ORG_KEY_STATUS=%s,\
                                     REGION=%s,\
                                     SEGMENT=%s,\
                                     ORG_KEY_CLASS_OF=%s,\
                                     CSM=%s,\
                                     CSM_EMAIL=%s,\
                                     DIRECTOR=%s,\
                                     DIRECTOR_EMAIL=%s,\
                                     SVP_RESPONSIBLE=%s,\
                                     SVP_EMAIL=%s,\
                                     VP_RESPONSIBLE=%s,\
                                     VP_EMAIL=%s,\
                                     NEW_BUSINESS_REP=%s,\
                                     NEW_BUSINESS_REP_EMAIL=%s,\
                                     BRAND_URL=%s,\
                                     UPDATED_DATE=%s\
                                     where PARENTORGKEY=%s and CHILDORGKEY=%s";
        val=(                           i['BPN'],
                                        i['PRIMARY_ID'],
                                        i['SFDC_ACCOUNTID'],
                                        i['OPPORTUNITY_ID'],
                                        i['OMS_ID'],
                                        i['OMS_PRODUCT_ORGANIZATION_ID'],
                                        i['MERCHANT_TYPE'],
                                        i['MERCH_ID'],
                                        i['MERCHANTNUMBER'],
                                        i['PARENTORG'],
                                        i['OMS_PARENTORG'],
                                        i['ORG_NAME'],
                                        i['MASTERPARENTNAME'],
                                        i['BRAND'],
                                        i['CHANNELPARTNER'],
                                        i['CSS_PRODUCT'],
                                        i['CSS_PRODUCT_TYPE'],
                                        i['CSS_MASTER_STATUS'],
                                        i['ORG_KEY_STATUS'],
                                        i['REGION'],
                                        i['SEGMENT'],
                                        i['ORG_KEY_CLASS_OF'],
                                        i['CSM'],
                                        i['CSM_EMAIL'],
                                        i['DIRECTOR'],
                                        i['DIRECTOR_EMAIL'],
                                        i['SVP_RESPONSIBLE'],
                                        i['SVP_EMAIL'],
                                        i['VP_RESPONSIBLE'],
                                        i['VP_EMAIL'],
                                        i['NEW_BUSINESS_REP'],
                                        i['NEW_BUSINESS_REP_EMAIL'],
                                        i['BRAND_URL'],
                                        #i['UPDATED_DATE'],
                                        date_time_str,
                                        i['PARENTORGKEY'],
                                        i['CHILDORGKEY']
									)
        conn.cursor().execute(sql_update,val)
    st.success(f"Changes Saved:rocket:")
