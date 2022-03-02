import sys
import ML
import datetime



ML.setConnectBinotelDB()
sessionID = ML.getSessionID()
firstInc = 1566248400  
startDay = datetime.date(2022, 1, 1)
delta = datetime.timedelta(days=1)
nowTS = ML.nowTS()


table_Binotel_Temp_CSAT = 'Temp_CSAT'
table_Binotel_CSAT_Intervals = 'CSAT_Intervals' 
table_Binotel_CSAT_Evalutions = 'CSAT_Evalutions'
table_Binotel_CSAT_Questions = 'CSAT_Questions'

connect_api_params = ML.connect_api_params

for k in range(len(connect_api_params['key'])):

    curDay = startDay
    fin = False
    while True:
        curDay += delta

        for HH in range(24):  # цикл по часам

            Key = int(curDay.strftime("%y%m%d")) * 100 + HH
            strKey = str(Key)
            begTS = round(datetime.datetime(curDay.year, curDay.month, curDay.day, HH).timestamp())

            if begTS > 1643241600:  # ML.nowTS() - 7200 * 1: # 1643241600 - 2022-01-27 00:00:00.000 ограничиваем загрузку по дате
                fin = True
                break

            exists = ML.qtyLines(table_Binotel_CSAT_Intervals,
                                 'where [IntervalID] = ' + strKey + " and  RetailNetwork = '" + str(
                                     connect_api_params['RetailNetwork'][k]).upper() + "'")

            if exists:
                continue


            ML.sleepSec(11)
            endTS = begTS + 3600 - 1

            CSAT = ML.BinatelPostParam('reviews/list-of-results-for-period', 'listOfResults',
                                            {'startTime': begTS, 'stopTime': endTS},
                                            connect_api_params['key'][k],
                                            connect_api_params['secret'][k])
            for key in CSAT:  # evaluatio
                e = CSAT[key]
                id = e['id']
                ML.runQuery("delete from [" + table_Binotel_CSAT_Evalutions + "] where [id] = " + str(
                    id) + " and  RetailNetwork = '" + str(connect_api_params['RetailNetwork'][k]).upper() + "'", 1)

                ML.runQuery(
                    "insert [" + table_Binotel_CSAT_Evalutions + "] (IntervalID , ID, Name, Type, GeneralCallID,"
                                                                 " ContactName, ContactPhone, ContactEmail, AddedAt, "
                                                                 "Updated, AddedAt_, RetailNetwork) "
                    + "values (" + strKey + ", " + str(id) + ", N'"
                    + e['name'] + "', N'" + e['type'] + "', " + e['generalCallID'] + ", N'" + e['contactName']
                    + "', N'" + e['contactPhone'] + "', N'" + e['contactEmail'] + "', "
                    + e['addedAt'] + ", getdate() " + ", N'" + ML.ConvertToDateTime(e['addedAt']) + "', '"
                    + str(connect_api_params['RetailNetwork'][k]).upper() + "' )", 1)

                listOfQuestions = e['listOfQuestions']

                ML.runQuery("delete from [" + table_Binotel_CSAT_Questions + "] where [id] = " + str(id)
                            + " and  RetailNetwork = '" + str(connect_api_params['RetailNetwork'][k]).upper() + "'", 1)

                for qnum, q in enumerate(listOfQuestions, 1):
                    myprint(qnum, q)
                    ML.runQuery("insert [" + table_Binotel_CSAT_Questions + "] values (" + str(id) + ", "
                                + str(qnum) + ", N'" + q['name'] + "', N'" + q['type'] + "', " + q['minMark']
                                + ", " + q['maxMark'] + ", " + q['result'] + ", getdate()" + ", '"
                                + str(connect_api_params['RetailNetwork'][k]).upper() + "' )", 1)

            Qty = len(CSAT)

            ML.runQuery('insert [' + table_Binotel_CSAT_Intervals + '] values (' + strKey + ',' + str(
                Qty) + ', getdate()' + ", '" + str(connect_api_params['RetailNetwork'][k]).upper() + "' )", 1)

        if fin:
            break

ML.closeConn()
sys.exit(0)
