Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading
Imports Utility

Module FactorsDailyLoad

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

        Dim factorIdArr As New ArrayList(CStr(System.Configuration.ConfigurationSettings.AppSettings("FactorIds")).Split(","))

        Dim startDate, endDate As DateTime
        If IsDate(System.Configuration.ConfigurationSettings.AppSettings("StartDate")) Then
            startDate = DateTime.Parse(System.Configuration.ConfigurationSettings.AppSettings("StartDate"))
        Else
            startDate = Nothing
        End If
        If IsDate(System.Configuration.ConfigurationSettings.AppSettings("EndDate")) Then
            endDate = DateTime.Parse(System.Configuration.ConfigurationSettings.AppSettings("EndDate"))
        Else
            endDate = Nothing
        End If

        Dim runRtnPreCalc As Boolean
        runRtnPreCalc = "TRUE".Equals(UCase(System.Configuration.ConfigurationSettings.AppSettings("ReturnPreCalc")))
        
        Dim rowCountOkay, chars, mqaOutputFiles As Boolean
        Dim i As Integer
        Dim dirArchiveStr, dirTempStr, dirMqaQueriesStr, dirMqaSpecQueriesStr As String
        Dim mqaExe, outputFile, paramFile1, paramFile2, batFile, s As String
        Dim removeStrings As String() = {"NULL", "ERR"}
        Dim factorCdArr, lineArr As String()
        Dim aDate, prevBusDate As DateTime
        Dim datesArr, filesArr, arrList As ArrayList
        Dim sr As StreamReader
        Dim sw As StreamWriter
        Dim file As FileInfo
        Dim dirTemp, dirArchive As DirectoryInfo
        Dim factorHash As Hashtable

        Try
            ConsoleWriteLine("------>>> FactorsDailyLoad: Main Begin ------>>>")

            If factorIdArr.Count() <= 0 Then
                ConsoleWriteLine("WARNING: No factors specified in config file!")
                ConsoleWriteLine("Exiting program...")
                GoTo WarningExit
            End If

            ConsoleWriteLine("Database Server = " + dbServerStr)
            ConsoleWriteLine("Database = " + dbStr)

            ConsoleWriteLine("------>>> FactorsDailyLoad: Initialization Begin ------>>>")

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn

            sqlQuery = "SELECT code, decode FROM decode WHERE item = 'DIR' AND code IN ('ARCHIVE','MQA_QUERIES','MQA_QUERIES_SPECIAL')" + ControlChars.NewLine
            sqlQuery += "UNION" + ControlChars.NewLine
            sqlQuery += "SELECT code, decode FROM decode WHERE item = 'FILE' AND code = 'QALPROC_EXE'"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                If rsReader.GetString(0).Equals("QALPROC_EXE") Then
                    mqaExe = ControlChars.Quote + rsReader.GetString(1) + ControlChars.Quote + " "
                ElseIf rsReader.GetString(0).Equals("MQA_QUERIES") Then
                    dirMqaQueriesStr = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("MQA_QUERIES_SPECIAL") Then
                    dirMqaSpecQueriesStr = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("ARCHIVE") Then
                    dirArchiveStr = rsReader.GetString(1)
                End If
            End While
            rsReader.Close()

            sqlQuery = "SELECT convert(varchar, getdate(), 112) + '_' + convert(varchar, getdate(), 108)"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                dirTempStr = "C:\temp_FactorsDailyLoad_" + rsReader.GetString(0).Replace(":", "") + "\"
            End While
            rsReader.Close()

            dirArchive = New DirectoryInfo(dirArchiveStr)
            dirTemp = New DirectoryInfo(dirTempStr)
            If Not dirTemp.Exists() Then
                dirTemp.Create()
            End If

            chars = False
            sqlQuery = "SELECT DISTINCT factor_id, factor_cd FROM factor" + ControlChars.NewLine
            sqlQuery += "WHERE factor_id IN ("
            For i = 0 To factorIdArr.Count() - 1
                factorIdArr.Item(i) = Trim(factorIdArr.Item(i))
                If CInt(factorIdArr.Item(i)).Equals(0) Then
                    chars = True
                End If
                If sqlQuery.EndsWith("(") Then
                    sqlQuery += CStr(factorIdArr.Item(i))
                Else
                    sqlQuery += ", " + CStr(factorIdArr.Item(i))
                End If
            Next
            sqlQuery += ")"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            factorHash = New Hashtable
            While rsReader.Read()
                factorHash.Add(CStr(rsReader.GetInt32(0)), rsReader.GetString(1) + ".QAL")
            End While
            rsReader.Close()

            If chars Then
                factorHash.Add("0", "CHARACTERISTICS.QAL")
            End If

            arrList = New ArrayList(factorHash.Keys())
            For Each s In arrList
                file = New FileInfo(dirMqaQueriesStr + factorHash.Item(s))
                If file.Exists() Then
                    factorHash.Item(s) = ControlChars.Quote + dirMqaQueriesStr + factorHash.Item(s) + ControlChars.Quote + " "
                Else
                    file = New FileInfo(dirMqaSpecQueriesStr + factorHash.Item(s))
                    If file.Exists() Then
                        factorHash.Item(s) = ControlChars.Quote + dirMqaSpecQueriesStr + factorHash.Item(s) + ControlChars.Quote + " "
                    Else
                        factorHash.Remove(s)
                    End If
                End If
            Next

            If factorHash.Count() <= 0 Then
                ConsoleWriteLine("ERROR: No legitimate factors found in config file!")
                ConsoleWriteLine("Exiting program...")
                Exit Try
            End If

            ConsoleWriteLine("<<<------ FactorsDailyLoad: Initialization End <<<------")
            ConsoleWriteLine("------>>> FactorsDailyLoad: GenerateParamFiles Begin ------>>>")

            sqlQuery = "EXEC business_date_get @DIFF=-1, @DATE_FORMAT=101"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                prevBusDate = CDate(rsReader.GetString(0))
                ConsoleWriteLine("Previous business day = " + prevBusDate.ToString("d"))
            End While
            rsReader.Close()

            If startDate = Nothing Then
                startDate = prevBusDate
            End If
            If endDate = Nothing Then
                endDate = prevBusDate
            End If

            ConsoleWriteLine()
            datesArr = New ArrayList

            While startDate <= endDate
                sqlQuery = "EXEC business_date_get @DIFF=0, @REF_DATE='" + startDate.ToString("d") + "', @DATE_FORMAT=101"
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                While rsReader.Read()
                    aDate = CDate(rsReader.GetString(0))
                    If startDate.Equals(aDate) Then
                        datesArr.Add(startDate)
                    End If
                End While
                rsReader.Close()
                startDate = startDate.AddDays(CDbl(1))
            End While

            If datesArr.Count() <= 0 Then
                ConsoleWriteLine("ERROR: No valid business dates found!")
                ConsoleWriteLine("Exiting program...")
                Exit Try
            End If

            datesArr.Sort()
            For i = 0 To datesArr.Count() - 1
                ConsoleWriteLine(dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "d.QAP")
                sw = New StreamWriter(dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "d.QAP")
                sw.WriteLine("$StartDate" + ControlChars.Tab + "$EndDate")
                sw.WriteLine(ControlChars.Quote + CDate(datesArr.Item(i)).ToString("d") + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + CDate(datesArr.Item(i)).ToString("d") + ControlChars.Quote)
                sw.Close()

                'NOTE TO SELF:
                'DO NOT RUN MSCI_FIF FACTOR HERE
                'DO IT IN THE BENCHMARK LOAD SCRIPT

                If Not (factorHash.ContainsKey("0") And factorHash.Count() = 1) Then
                    sqlQuery = "SELECT mqa_id FROM universe_makeup" + ControlChars.NewLine
                    sqlQuery += " WHERE universe_dt = '" + CDate(datesArr.Item(i)).ToString("d") + "'" + ControlChars.NewLine
                    sqlQuery += "   AND mqa_id IS NOT NULL" + ControlChars.NewLine
                    sqlQuery += "   AND mqa_id NOT LIKE '@%'" + ControlChars.NewLine
                    sqlQuery += "UNION" + ControlChars.NewLine
                    sqlQuery += "SELECT mqa_id FROM position" + ControlChars.NewLine
                    sqlQuery += " WHERE bdate = '" + CDate(datesArr.Item(i)).ToString("d") + "'" + ControlChars.NewLine
                    sqlQuery += "   AND mqa_id IS NOT NULL" + ControlChars.NewLine
                    sqlQuery += "   AND mqa_id NOT LIKE '@%'" + ControlChars.NewLine
                    sqlQuery += " ORDER BY mqa_id"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    rsReader = dbCommand.ExecuteReader()
                    ConsoleWriteLine(dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "s.QAP")
                    sw = New StreamWriter(dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "s.QAP")
                    sw.WriteLine("$ID")
                    While rsReader.Read()
                        sw.WriteLine(ControlChars.Quote + rsReader.GetString(0) + ControlChars.Quote)
                    End While
                    sw.Close()
                    rsReader.Close()
                End If

                If factorHash.ContainsKey("0") Then
                    sqlQuery = "SELECT x.mqa_id FROM (SELECT mqa_id FROM universe_makeup" + ControlChars.NewLine
                    sqlQuery += "                       WHERE universe_dt = '" + CDate(datesArr.Item(i)).ToString("d") + "'" + ControlChars.NewLine
                    sqlQuery += "                         AND mqa_id IS NOT NULL" + ControlChars.NewLine
                    sqlQuery += "                      UNION" + ControlChars.NewLine
                    sqlQuery += "                      SELECT mqa_id FROM position" + ControlChars.NewLine
                    sqlQuery += "                       WHERE bdate = '" + CDate(datesArr.Item(i)).ToString("d") + "'" + ControlChars.NewLine
                    sqlQuery += "                         AND mqa_id IS NOT NULL) x" + ControlChars.NewLine
                    sqlQuery += " WHERE x.mqa_id NOT IN (SELECT mqa_id FROM instrument_characteristics" + ControlChars.NewLine
                    sqlQuery += "                         WHERE bdate = '" + CDate(datesArr.Item(i)).ToString("d") + "'" + ControlChars.NewLine
                    sqlQuery += "                           AND mqa_id IS NOT NULL)" + ControlChars.NewLine
                    sqlQuery += "   AND x.mqa_id NOT LIKE '@%'" + ControlChars.NewLine
                    sqlQuery += " ORDER BY x.mqa_id"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    rsReader = dbCommand.ExecuteReader()
                    If rsReader.HasRows() Then
                        ConsoleWriteLine(dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "s_char.QAP")
                        sw = New StreamWriter(dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "s_char.QAP")
                        sw.WriteLine("$ID")
                        While rsReader.Read()
                            sw.WriteLine(ControlChars.Quote + rsReader.GetString(0) + ControlChars.Quote)
                        End While
                        sw.Close()
                    End If                    
                    rsReader.Close()
                End If
            Next

            dbConn.Close()
            ConsoleWriteLine("<<<------ FactorsDailyLoad: GenerateParamFiles End <<<------")
            ConsoleWriteLine("------>>> FactorsDailyLoad: RunQueries Begin ------>>>")

            batFile = dirTempStr + "MQA_QUERIES.BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            For Each s In factorHash.Keys()
                factorCdArr = CStr(factorHash.Item(s)).Split("\")
                factorCdArr = CStr(factorCdArr.GetValue(factorCdArr.Length() - 1)).Split(".")
                For i = 0 To datesArr.Count() - 1
                    paramFile1 = ControlChars.Quote + dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "d.QAP" + ControlChars.Quote + " "
                    outputFile = ControlChars.Quote + dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "_" + factorCdArr.GetValue(0) + ".CSV" + ControlChars.Quote + " "
                    If CInt(s).Equals(0) Then
                        paramFile2 = ControlChars.Quote + dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "s_char.QAP" + ControlChars.Quote + " "
                    Else
                        paramFile2 = ControlChars.Quote + dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "s.QAP" + ControlChars.Quote + " "
                    End If
                    If paramFile2.Contains("s_char.QAP") Then
                        file = New FileInfo(dirTempStr + CDate(datesArr.Item(i)).ToString("yyyyMMdd") + "s_char.QAP")
                        If file.Exists() Then
                            sw.WriteLine(mqaExe + CStr(factorHash.Item(s)) + outputFile + paramFile1 + paramFile2 + "/fc")
                        End If
                    Else
                        sw.WriteLine(mqaExe + CStr(factorHash.Item(s)) + outputFile + paramFile1 + paramFile2 + "/fc")
                    End If
                Next
            Next
            sw.Close()

            Do
                Shell(batFile, , True)
                mqaOutputFiles = True
                sr = New StreamReader(batFile)
                While sr.Peek() >= 0
                    lineArr = sr.ReadLine().Split(" ")
                    file = New FileInfo(CStr(lineArr.GetValue(2)).Replace(ControlChars.Quote, ""))
                    If Not file.Exists() Then
                        ConsoleWriteLine("MQA output file " + file.FullName() + " not found!")
                        mqaOutputFiles = False
                    End If
                    file = Nothing
                End While
                sr.Close()

                If mqaOutputFiles Then
                    Exit Do
                Else
                    ConsoleWriteLine("Sleeping for " + CStr(sysSleepIntervalMinutes) + " minute(s)...")
                    Thread.Sleep(sysSleepIntervalMinutes * sysMinute)
                    If Now() > sleepLimitTime Then
                        ConsoleWriteLine("ERROR: Current time beyond " + sleepLimitTime.TimeOfDay().ToString().Substring(0, 8))
                        ConsoleWriteLine("Terminating program...")
                        Exit Try
                    End If
                End If
            Loop While True

            ConsoleWriteLine("<<<------ FactorsDailyLoad: RunQueries End <<<------")
            ConsoleWriteLine("------>>> FactorsDailyLoad: DbLoad Begin ------>>>")

            filesArr = New ArrayList(dirTemp.GetFiles("*.CSV"))
            GenerateBcpFiles(filesArr, removeStrings, 1)

            filesArr = New ArrayList(dirTemp.GetFiles("*.BCP"))
            batFile = dirTempStr + "BCPBCP.BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            For Each file In filesArr
                If file.Name().Contains("CHARACTERISTICS") Then
                    sw.WriteLine("bcp QER..instrument_characteristics_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                Else
                    sw.WriteLine("bcp QER..instrument_factor_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                End If
            Next
            sw.Close()

            If Not (factorHash.ContainsKey("0") And factorHash.Count() = 1) Then
                sqlQuery = "DELETE instrument_factor_staging"
            Else
                sqlQuery = ""
            End If
            If factorHash.ContainsKey("0") Then
                If sqlQuery.Equals("") Then
                    sqlQuery = "DELETE instrument_characteristics_staging"
                Else
                    sqlQuery += ControlChars.NewLine + "DELETE instrument_characteristics_staging"
                End If
            End If

            dbConn.Open()
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn
            dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute
            rowCountOkay = False
            bcpAttemptCount = 0

            Do
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
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

            If Not (factorHash.ContainsKey("0") And factorHash.Count() = 1) Then
                sqlQuery = "EXEC instrument_factor_load @SOURCE_CD='MQA'"
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()
            End If

            If factorHash.ContainsKey("0") Then
                sqlQuery = "EXEC instrument_characteristics_load"
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()
            End If

            dbConn.Close()
            ConsoleWriteLine("<<<------ FactorsDailyLoad: DbLoad End <<<------")

            If runRtnPreCalc Then
                runRtnPreCalc = False
                arrList = New ArrayList(factorHash.Values())
                For Each s In arrList
                    If s.Contains("RETURN_1D") Then
                        runRtnPreCalc = True
                        Exit For
                    End If
                Next
            End If
            
            If runRtnPreCalc Then
                ConsoleWriteLine("------>>> FactorsDailyLoad: ReturnPreCalculation Begin ------>>>")
                dbConn.Open()
                dbCommand = New SqlCommand
                dbCommand.Connection = dbConn
                dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute

                For i = 0 To datesArr.Count() - 1
                    sqlQuery = "EXEC return_calc_daily @BDATE='" + CDate(datesArr.Item(i)).ToString("d") + "'"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                Next

                dbConn.Close()
                ConsoleWriteLine("<<<------ FactorsDailyLoad: ReturnPreCalculation End <<<------")
            End If

            If factorHash.ContainsKey("0") Then
                ConsoleWriteLine("------>>> FactorsDailyLoad: AverageVolumeUpdates Begin ------>>>")
                dbConn.Open()
                dbCommand = New SqlCommand
                dbCommand.Connection = dbConn
                dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute

                If datesArr.Count() = 1 Then
                    sqlQuery = "EXEC volume_avg_update @DATE1='" + datesArr.Item(0) + "', @NUM_DAYS=30"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                    sqlQuery = "EXEC volume_avg_update @DATE1='" + datesArr.Item(0) + "', @NUM_DAYS=60"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                    sqlQuery = "EXEC volume_avg_update @DATE1='" + datesArr.Item(0) + "', @NUM_DAYS=90"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                Else
                    sqlQuery = "EXEC volume_avg_update @DATE1='" + datesArr.Item(0) + "', @DATE2='" + datesArr.Item(datesArr.Count() - 1) + "', @NUM_DAYS=30"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                    sqlQuery = "EXEC volume_avg_update @DATE1='" + datesArr.Item(0) + "', @DATE2='" + datesArr.Item(datesArr.Count() - 1) + "', @NUM_DAYS=60"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                    sqlQuery = "EXEC volume_avg_update @DATE1='" + datesArr.Item(0) + "', @DATE2='" + datesArr.Item(datesArr.Count() - 1) + "', @NUM_DAYS=90"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                End If

                dbConn.Close()
                ConsoleWriteLine("<<<------ FactorsDailyLoad: AverageVolumeUpdates End <<<------")
            End If

            ConsoleWriteLine("------>>> FactorsDailyLoad: Archive Begin ------>>>")
            If Not dirArchive.Exists() Then
                dirArchive.Create()
            End If
            ConsoleWriteLine("Archive Directory = " + dirArchiveStr)
            ArchiveTempDir(dirTempStr, dirArchiveStr)
            ConsoleWriteLine("<<<------ FactorsDailyLoad: Archive End <<<------")
WarningExit:
            ConsoleWriteLine("<<<------ FactorsDailyLoad: Main End <<<------")
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
