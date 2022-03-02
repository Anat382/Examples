"""
REST API
***********************************
Get data
"""
import ML  
import sys

ML.setConnectBinotelDB()
sessionID = ML.getSessionID()
secs = 86400
firstInc = 1566248400  
nowTS = ML.nowTS()


# num_table = 2


def info( begTS, endTS):
    sec = endTS - begTS + 1


def getCalls(CallType, begTS, endTS, num_table):
    """
    Get data from API
    """
    param_num = num_table
    CallTypeStr = 'incoming' if CallType == 0 else 'outgoing'
    Calls = ML.BinatelPostParam('stats/' + CallTypeStr + '-calls-for-period', 'callDetails',
                                {'startTime': begTS, 'stopTime': endTS}, key_=ML.connect_api_params['key'][param_num],
                                secret_=ML.connect_api_params['secret'][param_num])
    Qty = len(Calls)
    ML.sleepSec(13)

    if Qty > 1999:
        midTS = int((begTS + endTS) / 2)
        getCalls(CallType, begTS, midTS)
        getCalls(CallType, midTS + 1, endTS)
        return

    Condition = 'where  [CallType] = ' + str(CallType) + ' and [StartTime] between ' + str(begTS) + ' and ' + str(endTS)
    Exists = ML.qtyLines(ML.table_Binotel_Calls[num_table], Condition)

    if Exists == Qty and endTS < nowTS - 3600 * 2:
        info(CallTypeStr, begTS, endTS, Qty, 'Уже+++')
        return

    info(CallTypeStr, begTS, endTS, Qty, 'Вносим')

    ML.clearTable(ML.table_Binotel_Temp_Calls[num_table])  # чистим временную

    for Key in Calls:
        Call = Calls[Key]
        CallID = Call['callID']
        CustomerData = Call['customerData']
        EmployeeData = Call['employeeData']
        PbxNumberData = Call['pbxNumberData']
        HistoryData = Call['historyData']  # пишем в временную


        ML.cursorInsert(ML.table_Binotel_Temp_Calls[num_table],
                        ML.addFields(Call, 'companyID:I,generalCallID:I,callID:I,'
                                           'startTime:I,callType:I,internalNumber,'
                                           'internalAdditionalData,externalNumber,'
                                           'waitsec:I,billsec:I,disposition,'
                                           'recordingStatus,isNewCall:I,whoHungUp') +
                        ML.addFields(CustomerData, 'id:I,name') + ML.addFields(EmployeeData, 'name,email')
                        + ML.addFields(PbxNumberData, 'number') + ML.addInt(len(HistoryData)) + ", '"
                        + str(ML.connect_api_params['RetailNetwork'][param_num]).upper() + "'")

        ML.clearTable(ML.table_Binotel_Calls_HistoryData[num_table], 'where [CallID] = ' + str(CallID))

        for Num, HistoryLine in enumerate(HistoryData, 1):
            try:
                HistoryEmployeeData = HistoryLine['employeeData']
            except:
                HistoryEmployeeData = {}

            ML.cursorInsert(ML.table_Binotel_Calls_HistoryData[num_table], ML.addInt(CallID) + ML.addInt(Num)
                            + ML.addFields(HistoryLine, 'waitsec:I,billsec:I,disposition,internalNumber,'
                                                        'internalAdditionalData') + ML.addFields(HistoryEmployeeData,
                                                                                                 'name,email') + ", '" + str(
                ML.connect_api_params['RetailNetwork'][param_num]).upper() + "'")

    ML.clearTable(ML.table_Binotel_Calls[num_table], 'where [CallID] in (select [CallID] from '
                  + ML.table_Binotel_Temp_Calls[num_table] + ')')  # удаляем по ID

    ML.cursorInsertTableScript(ML.table_Binotel_Calls[num_table],
                               "select [CompanyID], [GeneralCallID], [CallID], [StartTime], [CallType], "
                               "[InternalNumber], [InternalAdditionalData], [ExternalNumber], "
                               "[Waitsec], [Billsec], [Disposition], [RecordingStatus], [IsNewCall], [whoHungUp], "
                               "[CustomerData_ID], [CustomerData_Name], [EmployeeData_Name], [EmployeeData_Email], "
                               "[PbxNumberData_Number], [HistoryData_Qty] , [RetailNetwork] from ["
                               + ML.table_Binotel_Temp_Calls[num_table] + "] (nolock)")

    ML.commitConn()


# ------------------------------------------------------------------------------
maxDepth = ML.getNumArg(1, 5)  # действует ограничение API
finishTS = nowTS // 60 * 60
size = 3600 * 3


def calls_update(argv):
    """
    Calling a function from the CMD with a parameter
    insert calls
    :param argv:
    :return:
    """
    program, *args = argv
    ind = int(list(args)[0])
    for step in range(maxDepth):
        eTS = finishTS - step * size
        bTS = eTS - size
        eTS = eTS - 1
        print(step, bTS, eTS, ind)
        getCalls(0, bTS, eTS, ind)
        getCalls(1, bTS, eTS, ind)

    ML.cursorUpdateETL(sessionID, 'Reloaded: ' + str(ML.qtyLines(ML.table_Binotel_Calls[ind])) + ' '
                       + ML.table_Binotel_Calls[ind] + '; ' + str(
        ML.qtyLines(ML.table_Binotel_Calls_HistoryData[ind])) + ' '
                       + ML.table_Binotel_Calls_HistoryData[ind] + '')
    print('Ok')


if __name__ == '__main__':
    import sys

    exit(calls_update(sys.argv))
