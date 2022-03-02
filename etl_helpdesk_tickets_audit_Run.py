
import json
import HelpDesk as HD
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta
from time import sleep
import HelpDesk as HD

sql_conn = HD.GetConnection()
cursor = sql_conn.cursor()

load_table = 'tickets_audit'
records_count = 0

dates = HD.GetPeriodFromLogDF(load_table, sql_conn)
startDate = dates['DateStart'][0]

today_ = datetime.now()
today = today_.strftime("%Y-%m-%d %H:%M:%S")

endDate = today_.strftime("%Y-%m-%d 23:59:59")



def GetTicketsIdList() -> DataFrame:
    query = """
        SELECT distinct id FROM dbo.fTickets t WHERE ((t.date_created between  ? and ?) or (t.date_updated between  ? and ?))  
        and id not in (26976, 27240, 27284, 27245, 27798, 27842) 
        """
    return pd.read_sql(query, sql_conn, params=[startDate, endDate, startDate, endDate])


def delete_row_by_id_with_no_commit(id):
    cursor.execute(
        "delete from [HelpDesk].[dbo].[tickets_audit] where id = ?", id)


def delete_row_by_id_with_commit(id):
    cursor.execute(
        "delete from [HelpDesk].[dbo].[tickets_audit] where id = ?", id)
    cursor.commit()


def insert_row_with_commit(row):
    cursor.execute(
        "insert into [HelpDesk].[dbo].[tickets_audit] (id,event,text,user_id,user_name,date_created,id_ticket, log_date, load_id, is_updated) values(?, ?,?,?,?,?,?,?, ?, ?)",
        int(row['id']),
        row['event'],
        bytes(json.dumps(row['text']), "utf-8").decode("unicode_escape").replace("\t", ""),
        int(row['user_id']),
        row['user_name'],
        row['date_created'],
        str(id_l),
        today,
        str(load_id),
        0)
    cursor.commit()


def insert_page(dic: dict, records_count) -> int:
    df = DataFrame(dic['data'].items())
    for row in df[1]:
        records_count += 1
        delete_row_by_id_with_no_commit(row['id'])
        insert_row_with_commit(row)
    return records_count


def TicketsAuditStatusUpdate(cursor):
    cursor.execute('exec Tickets_audit_Status_update')
    cursor.commit()


if __name__ == '__main__':

    id_list = GetTicketsIdList()['id'].values.tolist()
    load_id = HD.GetLoadID(load_table, sql_conn, cursor);
    HD.write_log(load_table, 0, 0, '', load_id, 'start: ' + startDate + ' - ' + endDate, cursor)
    is_error = 0
    exc = ''
    exception_tickets = ''

    sleep_sec = 60
    max_limit_response = 295  # max response 300 / minutes
    count_response_h = 0
    for id_l in id_list:
        url = f'https://helpdesk.x100group.com/api/v2/tickets/{id_l}/audit'
        try:
            headers = HD.GetApiHeaders()
            params = {
                'from_date_created': '',
                'to_date_created': '',
            }
            params_main = params
            response = HD.response_get_data(url, params=params, headers=headers)

            data = json.loads(response.content.decode("utf-8"))

            records_count = insert_page(data, records_count)

            total_pages = data['pagination']['total_pages']
            page = 2
            count_response = 0
            while page <= total_pages:
                params = {'page': page, **params_main}
                response = ''

                response = HD.response_get_data(url, params=params, headers=headers)

                data = []
                data = json.loads(response.content.decode("utf-8"))
                records_count = insert_page(data, records_count)
                page += 1
                count_response += 1
                if count_response == max_limit_response:  # max response 300 / minutes
                    HD.sleepSec(sleep_sec)
                    count_response = 0

            count_response_h += 1
            if count_response_h == max_limit_response:  # max response 300 / minutes
                HD.sleepSec(sleep_sec)
                count_response_h = 0

        except Exception as e:
            exc = f'{e.args[0]}  => {__file__}'
            HD.Response_log(f'{url}, params:= {params}, headers:= {headers})', data, exc, cursor)
            exception_tickets += str(id_l) + ', '
            is_error = 1

    HD.write_log(load_table, records_count, is_error, exc, load_id,
                 'end: ' + startDate + ' - ' + endDate + '; ' + exception_tickets, cursor)
    cursor.commit()
    cursor.close()
    sql_conn.close()

