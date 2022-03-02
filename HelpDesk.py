import asyncio
import base64
import datetime
import requests
import time
import json
import pyodbc
import aiohttp
import pandas as pd
from pandas import DataFrame
from pandas.io.json import json_normalize
from time import sleep

connect_string = '*********************************************'


def getConnection() -> pyodbc.Connection:
    return pyodbc.connect(connect_string)


def getApiHeaders() -> list:
    user = '*********************************************'
    password =  '*********************************************'
    pair = '{}:{}'.format(user, password)
    auth = 'Basic {}'.format(base64.b64encode(bytes(pair, 'utf-8')).decode("utf-8"))
    headers = {'Authorization': auth}
    return headers


def gesponse_log(param_response: str, get_response: str, exception: str, cursor):
    cursor.execute(
        """
        DELETE FROM dbo.log_response WHERE CAST(Sys_Datetime as date) < CAST( DATEADD( day, -3, GETDATE() ) as DATE);
        INSERT INTO dbo.log_response([Param_response],[Get_response],[Exception] )VALUES(?, ?, ?)
        """,
        str(param_response),
        str(get_response),
        str(exception)
    )
    cursor.commit()


def response_get_data(url, params, headers):
    """
    Check  response and send request befor 10 attempts
    :param url:
    :param params:
    :param headers:
    :return:
    """
    response_code = 0
    count = 0
    while True:
        try:
            response = requests.get(url, params=params, headers=headers)
        except Exception as e:
            exc = f'{e.args[0]}  => {__file__}'
            response_code = 400

        if count == 10:
            break

        if response.status_code != 200 or response_code == 400:
            count += 1
            # print(count)
            sleepSec(60)
        else:
            return response
            break



def sleepSec(wait):
    time.sleep(wait)


def TestAPI(url, params, headers):  # add code Ponomarev 13112021
    """
    :param url:
    :param params:
    :param headers:
    :return: code
    """
    response = requests.get(url, params=params, headers=headers)
    code = response.status_code
    assert code == 200, f'Error: {__file__}, {code}, Can not connected to {url} \n {params} \n {headers}'
    return code


def GetPeriodByTableNameDF(table_name, connect) -> DataFrame:
    query = "SELECT format([DateStart], 'yyyy-MM-dd HH:mm:ss') as DateStart, format([DateEnd], 'yyyy-MM-dd HH:mm:ss') as DateEnd FROM [HelpDesk].[dbo].[Configure] WHERE  Actual = 1 and TableName = ?"
    return pd.read_sql(query, connect, params=[table_name])


def GetPeriodFromLogDF(table_name, connect) -> DataFrame:
    query = """
        SELECT top 1 format(dateadd(hour, -1, log_date), 'yyyy-MM-dd HH:mm:ss') as DateStart, format(dateadd(hour, 1, log_date), 'yyyy-MM-dd HH:mm:ss') as DateEnd
        FROM [HelpDesk].[dbo].[Log_History] 
        WHERE is_error  = 0 and log_table = ?
        order by id desc
        """
    return pd.read_sql(query, connect, params=[table_name])


def write_load_info(table_name: str, cursor):
    cursor.execute(
        "INSERT INTO [dbo].[Load_info]([table_name] )VALUES(?)",
        table_name)
    cursor.commit()


def GetLoadID(table_name, connect, cursor) -> int:
    write_load_info(table_name, cursor)
    query = "select max(load_id) as max_load_id from Load_info"
    res = pd.read_sql(query, connect)
    return (res['max_load_id'][0])


def write_log(log_table: str, records_count: int, is_error: bool, exception: str, load_id: int, add_info: str, cursor):
    cursor.execute(
        "INSERT INTO [dbo].[Log_History]([log_table],[records_count],[log_date],[is_error],[log_errors],[load_id],[add_info])VALUES(?, ?, ?, ?, ?, ?, ?)",
        log_table,
        str(records_count),
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        is_error,
        exception,
        str(load_id),
        add_info
    )
    cursor.commit()


def ins_users_log(records_count, is_error, exceptions, load_id, add_info, cursor):
    write_log('users', records_count, is_error, exceptions, load_id, add_info, cursor)


def listToString(paramList) -> str:
    res = ""
    for item in paramList:
        res += str(item)
    return res
