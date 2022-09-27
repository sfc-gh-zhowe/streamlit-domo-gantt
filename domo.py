from datetime import *
import streamlit as st
import altair as alt
import snowflake.connector
from connector import snowflake_login

connection = None

def main():
     global connection
     st.set_page_config(layout="wide")

     with st.sidebar:
        connection = snowflake_login()

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection(connectionName):
     #print(f' Kwargs: {st.secrets["snowflake"]}' )
     return snowflake.connector.connect(**st.secrets[connectionName])

queryTagList="""
SELECT SUBSTR(query_tag, 1, 16) domo_tag, MIN(start_time) min_start, MAX(start_time) max_start, count(*) 
FROM snowflake.account_usage.query_history
WHERE start_time between %(startDateTime)s AND %(endDateTime)s
AND warehouse_name = %(warehouseName)s
AND user_name = %(userName)s
AND query_type = 'SELECT' and query_text <> 'select 1'
GROUP BY domo_tag
ORDER BY min_start;
"""

queryResultList="""
SELECT session_id, query_id, start_time, end_time, query_tag, rows_produced, query_text
FROM snowflake.account_usage.query_history
WHERE start_time between %(startDateTime)s AND %(endDateTime)s
AND warehouse_name = %(warehouseName)s
AND user_name = %(userName)s
AND SUBSTR(query_tag, 1, 16) = %(tag)s
AND query_type = 'SELECT' and query_text <> 'select 1'
ORDER BY session_id, start_time;
"""

def select_inputs():
    warehouseName = st.text_input('Warehouse Name', value='FIN_VIZ')
    userName = st.text_input('UserName', value='ALICIA_FERNANDEZ@COLPAL.COM')
    queryDate = st.date_input('Query Date', value = date(2022, 8, 1))
    (startTime, endTime) = st.slider('TimeSpan', min_value=time(0,0,0), max_value=time(23,59,59), value=(time(15,0,0), time(16,0,0)), step=timedelta(minutes=1))

    startDateTime = datetime(queryDate.year, queryDate.month, queryDate.day, startTime.hour, startTime.minute, startTime.second)
    endDateTime = datetime(queryDate.year, queryDate.month, queryDate.day, endTime.hour, endTime.minute, endTime.second)

    with connection.cursor() as cur:
        cur.execute(queryTagList, { 'startDateTime': startDateTime, 'endDateTime': endDateTime, 'warehouseName': warehouseName, 'userName': userName })
        tagList = cur.fetch_pandas_all()

    col1, col2 = st.columns(2)
    tag = col1.selectbox('Select Batch', options=tagList)
    col2.write(tagList)

    with connection.cursor() as cur:
        cur.execute(queryResultList, { 'startDateTime': startDateTime, 'endDateTime': endDateTime, 'warehouseName': warehouseName, 'userName': userName, 'tag': tag})
        results = cur.fetch_pandas_all()

    bar = alt.Chart(results).mark_bar().encode(
        x='START_TIME:T',
        x2='END_TIME:T',
        y='SESSION_ID:N',
        tooltip=[alt.Tooltip('QUERY_ID:N', title='QueryID')
            , alt.Tooltip('QUERY_TAG:N', title='QueryTag')
            , alt.Tooltip('START_TIME:T', format = ('%Y-%m-%d %H:%M:%S'), title='StartTime')
            , alt.Tooltip('END_TIME:T', format = ('%Y-%m-%d %H:%M:%S'), title='EndTime')
            , alt.Tooltip('QUERY_TEXT:N', title='QueryText')
        ]
    )
    tick = alt.Chart(results).mark_tick(
        color='red',
        thickness=2,
        size=20,  # controls height of tick.
    ).encode(
        #x='START_TIME:T',
        x='END_TIME:T',
        y='SESSION_ID:N'
    )
    chart = bar + tick

    st.write(chart)

    st.write(results)

main()
if (connection != None):
    st.markdown('<style>#vg-tooltip-element{z-index: 1000051}</style>', unsafe_allow_html=True) #for fullscreen chart tooltips! https://discuss.streamlit.io/t/tool-tips-in-fullscreen-mode-for-charts/6800/7

    select_inputs()
    
