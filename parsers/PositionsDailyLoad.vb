Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading
Imports Utility

Module PositionsDailyLoad

    'KNOWN ISSUES:
    'THIS SCRIPT DOES NOT HANDLE INTERNATIONAL POSITIONS - IT JOINS ON CUSIP ONLY

    Sub Main()
        Dim dbServerStr As String = System.Configuration.ConfigurationSettings.AppSettings("DbServer")
        Dim dbStr As String = System.Configuration.ConfigurationSettings.AppSettings("Database")
        Dim dbConnStr As String = "Integrated Security=SSPI; Data Source=" + dbServerStr + "; Initial Catalog=" + dbStr
        Dim dbConn As SqlConnection = New SqlConnection(dbConnStr)

        Dim sqlQuery As String
        Dim dbCommand As SqlCommand
        Dim rsReader As SqlDataReader

        Dim bcpAttemptsMax, bcpAttemptCount As Integer
        If IsNumeric(System.Configuration.ConfigurationSettings.AppSettings("BcpAttemptsMax")) Then
            bcpAttemptsMax = CInt(System.Configuration.ConfigurationSettings.AppSettings("BcpAttemptsMax"))
        Else
            bcpAttemptsMax = 10
        End If

        Dim sleepLimitTimeArr As String() = System.Configuration.ConfigurationSettings.AppSettings("SleepLimitTime").Split(":")
        If sleepLimitTimeArr.Length() < 3 Then
            sleepLimitTimeArr = "10:30:00".Split(":")
        End If

        Dim sleepLimitTime As DateTime = Now().Date()
        sleepLimitTime = sleepLimitTime.AddHours(CDbl(sleepLimitTimeArr.GetValue(0)))
        sleepLimitTime = sleepLimitTime.AddMinutes(CDbl(sleepLimitTimeArr.GetValue(1)))
        sleepLimitTime = sleepLimitTime.AddSeconds(CDbl(sleepLimitTimeArr.GetValue(2)))

        Dim sysSleepIntervalMinutes As Integer
        Dim dbTimeoutMinutes As Integer
        Dim sysMinute As Integer = 60 * 1000
        Dim dbMinute As Integer = 60

        If IsNumeric(System.Configuration.ConfigurationSettings.AppSettings("DbTimeoutMinutes")) Then
            dbTimeoutMinutes = CInt(System.Configuration.ConfigurationSettings.AppSettings("DbTimeoutMinutes"))
        Else
            dbTimeoutMinutes = 30
        End If

        If IsNumeric(System.Configuration.ConfigurationSettings.AppSettings("SleepIntervalMinutes")) Then
            sysSleepIntervalMinutes = CInt(System.Configuration.ConfigurationSettings.AppSettings("SleepIntervalMinutes"))
        Else
            sysSleepIntervalMinutes = 10
        End If

        Dim runDate As DateTime
        If IsDate(System.Configuration.ConfigurationSettings.AppSettings("RunDate")) Then
            runDate = DateTime.Parse(System.Configuration.ConfigurationSettings.AppSettings("RunDate"))
        Else
            runDate = Now.Date()
        End If

        Dim rowCountOkay As Boolean
        Dim dirTempStr, dirArchiveStr, batFile As String
        Dim mqaExe, mqaCusipQuery, mqaCharQuery, outputFile, paramFile1, paramFile2, iStr, jStr As String
        Dim removeStrings As String() = {"NULL", "ERR"}
        Dim prevBusDate As DateTime
        Dim iArr, jArr, filesArr As ArrayList
        Dim sw As StreamWriter
        Dim file As FileInfo
        Dim dirTemp, dirArchive As DirectoryInfo

        Try
            ConsoleWriteLine("------>>> PositionsDailyLoad: Main Begin ------>>>")

            ConsoleWriteLine("Database Server = " + dbServerStr)
            ConsoleWriteLine("Database = " + dbStr)

            ConsoleWriteLine("------>>> PositionsDailyLoad: Initialization Begin ------>>>")

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn
            dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute

            sqlQuery = "SELECT code, decode FROM decode WHERE item = 'DIR' AND code IN ('ARCHIVE','MQA_QUERIES_SPECIAL')" + ControlChars.NewLine
            sqlQuery += "UNION" + ControlChars.NewLine
            sqlQuery += "SELECT code, decode FROM decode WHERE item = 'FILE' AND code = 'QALPROC_EXE'"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                If rsReader.GetString(0).Equals("QALPROC_EXE") Then
                    mqaExe = ControlChars.Quote + rsReader.GetString(1) + ControlChars.Quote + " "
                ElseIf rsReader.GetString(0).Equals("MQA_QUERIES_SPECIAL") Then
                    mqaCusipQuery = ControlChars.Quote + rsReader.GetString(1) + "CUSIP2MQA_ID.QAL" + ControlChars.Quote + " "
                    mqaCharQuery = ControlChars.Quote + rsReader.GetString(1) + "CHARACTERISTICS.QAL" + ControlChars.Quote + " "
                ElseIf rsReader.GetString(0).Equals("ARCHIVE") Then
                    dirArchiveStr = rsReader.GetString(1)
                End If
            End While
            rsReader.Close()

            sqlQuery = "EXEC business_date_get @REF_DATE='" + runDate.ToString("d") + "', @DIFF=-1, @DATE_FORMAT=101"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                ConsoleWriteLine("Run date = " + runDate.ToString("d"))
                prevBusDate = CDate(rsReader.GetString(0))
                ConsoleWriteLine("Previous business day = " + prevBusDate.ToString("d"))
            End While
            rsReader.Close()

            sqlQuery = "SELECT convert(varchar, getdate(), 112) + '_' + convert(varchar, getdate(), 108)"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                dirTempStr = "C:\temp_PositionsDailyLoad_" + rsReader.GetString(0).Replace(":", "") + "\"
            End While
            rsReader.Close()

            dirArchive = New DirectoryInfo(dirArchiveStr)
            dirTemp = New DirectoryInfo(dirTempStr)
            If Not dirTemp.Exists() Then
                dirTemp.Create()
            End If

            ConsoleWriteLine("<<<------ PositionsDailyLoad: Initialization End <<<------")
            ConsoleWriteLine("------>>> PositionsDailyLoad: LoadPositions Begin ------>>>")

            sqlQuery = "EXEC position_load @BDATE='" + prevBusDate.ToString("d") + "'"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            ConsoleWriteLine("<<<------ PositionsDailyLoad: LoadPositions End <<<------")
            ConsoleWriteLine("------>>> PositionsDailyLoad: MQA and DbLoad Begin ------>>>")

            ConsoleWriteLine(dirTempStr + "DATE.QAP")
            sw = New StreamWriter(dirTempStr + "DATE.QAP")
            sw.WriteLine("$Date")
            sw.WriteLine(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote)
            sw.Close()

            sqlQuery = "SELECT DISTINCT cusip FROM position" + ControlChars.NewLine
            sqlQuery += " WHERE bdate='" + prevBusDate.ToString("d") + "' AND cusip IS NOT NULL" + ControlChars.NewLine
            sqlQuery += " ORDER BY cusip"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            If rsReader.HasRows() Then
                ConsoleWriteLine(dirTempStr + "CUSIP.QAP")
                sw = New StreamWriter(dirTempStr + "CUSIP.QAP")
                sw.WriteLine("$CUSIP")
                While rsReader.Read()
                    sw.WriteLine(ControlChars.Quote + rsReader.GetString(0) + ControlChars.Quote)
                End While
                sw.Close()
                rsReader.Close()
                dbConn.Close()
            Else
                ConsoleWriteLine("ERROR: No cusips found in position table on " + prevBusDate.ToString("d") + "!")
                Exit Try
            End If

            batFile = dirTempStr + "Cusip2MqaId.BAT"

            outputFile = ControlChars.Quote + dirTempStr + "CusipMqaId.CSV" + ControlChars.Quote + " "
            paramFile1 = ControlChars.Quote + dirTempStr + "CUSIP.QAP" + ControlChars.Quote + " "
            paramFile2 = ControlChars.Quote + dirTempStr + "DATE.QAP" + ControlChars.Quote + " "

            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            sw.WriteLine(mqaExe + mqaCusipQuery + outputFile + paramFile1 + paramFile2 + "/fq")
            sw.Close()

            Do
                Shell(batFile, , True)
                file = New FileInfo(dirTempStr + "CusipMqaId.CSV")
                If file.Exists() Then
                    Exit Do
                Else
                    ConsoleWriteLine("MQA output file " + file.FullName() + " not found!")
                    ConsoleWriteLine("Sleeping for " + CStr(sysSleepIntervalMinutes) + " minute(s)...")
                    Thread.Sleep(sysSleepIntervalMinutes * sysMinute)
                    If Now() > sleepLimitTime Then
                        ConsoleWriteLine("ERROR: Current time beyond " + sleepLimitTime.TimeOfDay().ToString().Substring(0, 8))
                        ConsoleWriteLine("Terminating program...")
                        Exit Try
                    End If
                End If
                file = Nothing
            Loop While True

            filesArr = New ArrayList(dirTemp.GetFiles("CusipMqaId*.CSV"))
            GenerateBcpFiles(filesArr, removeStrings, 1)

            batFile = dirTempStr + "bcpbcp01.BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            sw.WriteLine("bcp QER..cusip2mqa_id_staging in " + dirTempStr + "CusipMqaId.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            sw.Close()

            sqlQuery = "DELETE cusip2mqa_id_staging"

            dbConn.Open()
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn
            dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute
            rowCountOkay = False
            bcpAttemptCount = 0

            Do
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()

                Shell(batFile, , True)
                rowCountOkay = BcpConfirmRowCount(batFile)
                ConsoleWriteLine("BcpConfirmRowCount = " + UCase(rowCountOkay.ToString()))

                If bcpAttemptCount >= bcpAttemptsMax Then
                    ConsoleWriteLine("ERROR: BCP failed after " + CStr(bcpAttemptsMax) + " attempts!")
                    Exit Try
                End If

                bcpAttemptCount += 1
            Loop While Not rowCountOkay

            sqlQuery = "SELECT DISTINCT mqa_id FROM cusip2mqa_id_staging" + ControlChars.NewLine
            sqlQuery += " WHERE mqa_id NOT IN (SELECT mqa_id FROM instrument_characteristics WHERE bdate = '" + prevBusDate.ToString("d") + "' AND mqa_id IS NOT NULL)" + ControlChars.NewLine
            sqlQuery += "   AND mqa_id IS NOT NULL" + ControlChars.NewLine
            sqlQuery += " ORDER BY mqa_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            If rsReader.HasRows() Then
                ConsoleWriteLine(dirTempStr + "MQA_ID.QAP")
                sw = New StreamWriter(dirTempStr + "MQA_ID.QAP")
                sw.WriteLine("$ID")
                While rsReader.Read()
                    sw.WriteLine(ControlChars.Quote + rsReader.GetString(0) + ControlChars.Quote)
                End While
                sw.Close()
            End If
            rsReader.Close()
            dbConn.Close()

            file = New FileInfo(dirTempStr + "MQA_ID.QAP")
            If Not file.Exists() Then
                ConsoleWriteLine("WARNING: no new mqa_id's found in cusip2mqa_id_staging table!")
                GoTo UpdatePosition
            End If

            ConsoleWriteLine(dirTempStr + "DATE_CHAR.QAP")
            sw = New StreamWriter(dirTempStr + "DATE_CHAR.QAP")
            sw.WriteLine("$StartDate" + ControlChars.Tab + "$EndDate")
            sw.WriteLine(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + prevBusDate + ControlChars.Quote)
            sw.Close()

            batFile = dirTempStr + "CHARACTERISTICS.BAT"

            outputFile = ControlChars.Quote + dirTempStr + "CHARACTERISTICS.CSV" + ControlChars.Quote + " "
            paramFile1 = ControlChars.Quote + dirTempStr + "DATE_CHAR.QAP" + ControlChars.Quote + " "
            paramFile2 = ControlChars.Quote + dirTempStr + "MQA_ID.QAP" + ControlChars.Quote + " "

            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            sw.WriteLine(mqaExe + mqaCharQuery + outputFile + paramFile1 + paramFile2 + "/fc")
            sw.Close()

            Do
                Shell(batFile, , True)
                file = New FileInfo(dirTempStr + "CHARACTERISTICS.CSV")
                If file.Exists() Then
                    Exit Do
                Else
                    ConsoleWriteLine("MQA output file " + file.FullName() + " not found!")
                    ConsoleWriteLine("Sleeping for " + CStr(sysSleepIntervalMinutes) + " minute(s)...")
                    Thread.Sleep(sysSleepIntervalMinutes * sysMinute)
                    If Now() > sleepLimitTime Then
                        ConsoleWriteLine("ERROR: Current time beyond " + sleepLimitTime.TimeOfDay().ToString().Substring(0, 8))
                        ConsoleWriteLine("Terminating program...")
                        Exit Try
                    End If
                End If
                file = Nothing
            Loop While True

            filesArr = New ArrayList(dirTemp.GetFiles("CHARACTERISTICS*.CSV"))
            GenerateBcpFiles(filesArr, removeStrings, 1)

            batFile = dirTempStr + "bcpbcp02.BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            sw.WriteLine("bcp QER..instrument_characteristics_staging in " + dirTempStr + "CHARACTERISTICS.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            sw.Close()

            sqlQuery = "DELETE instrument_characteristics_staging"

            dbConn.Open()
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn
            dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute
            rowCountOkay = False
            bcpAttemptCount = 0

            Do
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()

                Shell(batFile, , True)
                rowCountOkay = BcpConfirmRowCount(batFile)
                ConsoleWriteLine("BcpConfirmRowCount = " + UCase(rowCountOkay.ToString()))

                If bcpAttemptCount >= bcpAttemptsMax Then
                    ConsoleWriteLine("ERROR: BCP failed after " + CStr(bcpAttemptsMax) + " attempts!")
                    Exit Try
                End If

                bcpAttemptCount += 1
            Loop While Not rowCountOkay

            sqlQuery = "EXEC instrument_characteristics_load"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()
UpdatePosition:
            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn
            dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute

            sqlQuery = "EXEC unknown_security_load @BDATE='" + prevBusDate.ToString("d") + "'"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            sqlQuery = "UPDATE position" + ControlChars.NewLine
            sqlQuery += "   SET mqa_id = s.mqa_id" + ControlChars.NewLine
            sqlQuery += "  FROM cusip2mqa_id_staging s" + ControlChars.NewLine
            sqlQuery += " WHERE position.bdate = '" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
            sqlQuery += "   AND position.cusip = s.input_cusip"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")

            iArr = New ArrayList()
            iArr.Add("ticker")
            iArr.Add("sedol")
            iArr.Add("isin")
            iArr.Add("gv_key")

            jArr = New ArrayList()
            jArr.Add("mqa_id")
            jArr.Add("cusip")

            For Each iStr In iArr
                For Each jStr In jArr
                    sqlQuery = "UPDATE position" + ControlChars.NewLine
                    sqlQuery += "   SET " + iStr + " = i." + iStr + ControlChars.NewLine
                    sqlQuery += "  FROM instrument_characteristics i" + ControlChars.NewLine
                    sqlQuery += " WHERE position.bdate = '" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
                    sqlQuery += "   AND position." + iStr + " IS NULL" + ControlChars.NewLine
                    sqlQuery += "   AND position." + jStr + " = i." + jStr + ControlChars.NewLine
                    sqlQuery += "   AND i.bdate = '" + prevBusDate.ToString("d") + "'"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")
                Next
            Next

            sqlQuery = "UPDATE position" + ControlChars.NewLine
            sqlQuery += "   SET ticker = cusip" + ControlChars.NewLine
            sqlQuery += " WHERE cusip = '_USD'" + ControlChars.NewLine
            sqlQuery += "   AND bdate = '" + prevBusDate.ToString("d") + "'"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")

            sqlQuery = "EXEC position_sector_classify @BDATE='" + prevBusDate.ToString("d") + "'"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()
            dbConn.Close()

            ConsoleWriteLine("<<<------ PositionsDailyLoad: MQA and DbLoad End <<<------")
            ConsoleWriteLine("------>>> PositionsDailyLoad: Archive Begin ------>>>")
            If Not dirArchive.Exists() Then
                dirArchive.Create()
            End If
            ConsoleWriteLine("Archive Directory = " + dirArchiveStr)
            ArchiveTempDir(dirTempStr, dirArchiveStr)
            ConsoleWriteLine("<<<------ PositionsDailyLoad: Archive End <<<------")
            ConsoleWriteLine("<<<------ PositionsDailyLoad: Main End <<<------")
        Catch exSQL As SqlException
            ConsoleWriteLine()
            Console.WriteLine(exSQL.ToString())
        Catch ex As Exception
            ConsoleWriteLine()
            Console.WriteLine(ex.ToString())
        Finally
            If Not dbConn Is Nothing Then
                If dbConn.State() = ConnectionState.Open Then
                    dbConn.Close()
                End If
            End If
        End Try
    End Sub

End Module
