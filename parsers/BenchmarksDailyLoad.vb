Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading
Imports Utility

Module BenchmarksDailyLoad

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

        Dim bm, bmMsci, bmEqCm As Boolean
        bm = "TRUE".Equals(UCase(System.Configuration.ConfigurationSettings.AppSettings("Benchmarks")))
        bmMsci = "TRUE".Equals(UCase(System.Configuration.ConfigurationSettings.AppSettings("MsciBenchmarks")))
        bmEqCm = "TRUE".Equals(UCase(System.Configuration.ConfigurationSettings.AppSettings("EquityCommonBenchmarks")))

        Dim rowCountOkay As Boolean
        Dim i As Integer
        Dim dirArchiveStr, dirTempStr, batFile, aString As String
        Dim mqaExe, mqaUniverseQuery, mqaUniverseMsciQuery, outputFile, paramFile1, paramFile2 As String
        Dim removeStrings As String() = {"NULL", "ERR"}
        Dim aDate, prevBusDate As DateTime
        Dim datesArr, filesArr As ArrayList
        Dim sw As StreamWriter
        Dim file As FileInfo
        Dim dirTemp, dirArchive As DirectoryInfo
        Dim universeHash, universeMsciHash As Hashtable

        Try
            ConsoleWriteLine("------>>> BenchmarksDailyLoad: Main Begin ------>>>")

            If Not (bm Or bmMsci Or bmEqCm) Then
                ConsoleWriteLine("WARNING: No benchmarks specified in configuration file!")
                ConsoleWriteLine("Exiting program...")
                GoTo MainExit
            End If

            ConsoleWriteLine("Database Server = " + dbServerStr)
            ConsoleWriteLine("Database = " + dbStr)

            ConsoleWriteLine("------>>> BenchmarksDailyLoad: Initialization Begin ------>>>")

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn

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
                    mqaUniverseQuery = ControlChars.Quote + rsReader.GetString(1) + "UNIVERSE_BY_BENCHMARK.QAL" + ControlChars.Quote + " "
                    mqaUniverseMsciQuery = ControlChars.Quote + rsReader.GetString(1) + "UNIVERSE_MSCI_INDEX.QAL" + ControlChars.Quote + " "
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
                dirTempStr = "C:\temp_BenchmarksDailyLoad_" + rsReader.GetString(0).Replace(":", "") + "\"
            End While
            rsReader.Close()

            dirArchive = New DirectoryInfo(dirArchiveStr)
            dirTemp = New DirectoryInfo(dirTempStr)
            If Not dirTemp.Exists() Then
                dirTemp.Create()
            End If

            ConsoleWriteLine("<<<------ BenchmarksDailyLoad: Initialization End <<<------")
            ConsoleWriteLine("------>>> BenchmarksDailyLoad: GenerateParamFiles Begin ------>>>")

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
            ConsoleWriteLine(dirTempStr + "DATE.QAP")
            sw = New StreamWriter(dirTempStr + "DATE.QAP")
            sw.WriteLine("$Date")
            For i = 0 To datesArr.Count() - 1
                sw.WriteLine(ControlChars.Quote + CDate(datesArr.Item(i)).ToString("d") + ControlChars.Quote)
            Next
            sw.Close()
            sw = Nothing

            universeHash = Nothing
            universeMsciHash = Nothing

            If bm Then
                sqlQuery = "SELECT d.mqa_ticker, d.universe_cd" + ControlChars.NewLine
                sqlQuery += "  FROM benchmark b, universe_def d" + ControlChars.NewLine
                sqlQuery += " WHERE b.universe_id = d.universe_id" + ControlChars.NewLine
                sqlQuery += "   AND d.mqa_ticker IS NOT NULL" + ControlChars.NewLine
                sqlQuery += "   AND d.universe_cd NOT LIKE 'MSCI%'"
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                universeHash = New Hashtable
                While rsReader.Read()
                    universeHash.Add(rsReader.GetString(0), rsReader.GetString(1))
                End While
                rsReader.Close()

                ConsoleWriteLine(dirTempStr + "BM.QAP")
                sw = New StreamWriter(dirTempStr + "BM.QAP")
                sw.WriteLine("$Index" + ControlChars.Tab + "$Benchmark_cd")
                For Each aString In universeHash.Keys()
                    sw.WriteLine(ControlChars.Quote + aString + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + universeHash.Item(aString) + ControlChars.Quote)
                Next
                sw.Close()
                sw = Nothing
            End If

            If bmMsci Then
                sqlQuery = "SELECT mqa_ticker, universe_cd" + ControlChars.NewLine
                sqlQuery += "  FROM universe_def" + ControlChars.NewLine
                sqlQuery += " WHERE universe_cd LIKE 'MSCI%'" + ControlChars.NewLine
                sqlQuery += "   AND mqa_ticker IS NOT NULL"
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                universeMsciHash = New Hashtable
                While rsReader.Read()
                    universeMsciHash.Add(rsReader.GetString(0), rsReader.GetString(1))
                End While
                rsReader.Close()

                ConsoleWriteLine(dirTempStr + "BM_MSCI.QAP")
                sw = New StreamWriter(dirTempStr + "BM_MSCI.QAP")
                sw.WriteLine("$Index" + ControlChars.Tab + "$Benchmark_cd")
                For Each aString In universeMsciHash.Keys()
                    sw.WriteLine(ControlChars.Quote + aString + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + universeMsciHash.Item(aString) + ControlChars.Quote)
                Next
                sw.Close()
                sw = Nothing
            End If

            dbConn.Close()
            ConsoleWriteLine("<<<------ BenchmarksDailyLoad: GenerateParamFiles End <<<------")
            ConsoleWriteLine("------>>> BenchmarksDailyLoad: RunQuery Begin ------>>>")

            If bm AndAlso universeHash.Count() > 0 Then
                outputFile = ControlChars.Quote + dirTempStr + "UNIVERSE_BY_BENCHMARK.CSV" + ControlChars.Quote + " "
                paramFile1 = ControlChars.Quote + dirTempStr + "DATE.QAP" + ControlChars.Quote + " "
                paramFile2 = ControlChars.Quote + dirTempStr + "BM.QAP" + ControlChars.Quote + " "

                batFile = dirTempStr + "UNIVERSE.BAT"
                ConsoleWriteLine(batFile)
                sw = New StreamWriter(batFile)
                sw.WriteLine(mqaExe + mqaUniverseQuery + outputFile + paramFile1 + paramFile2 + "/fq")
                sw.Close()
                sw = Nothing

                Do
                    Shell(batFile, , True)
                    file = New FileInfo(dirTempStr + "UNIVERSE_BY_BENCHMARK.CSV")
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
            End If

            If bmMsci AndAlso universeMsciHash.Count() > 0 Then
                outputFile = ControlChars.Quote + dirTempStr + "UNIVERSE_MSCI_INDEX.CSV" + ControlChars.Quote + " "
                paramFile1 = ControlChars.Quote + dirTempStr + "DATE.QAP" + ControlChars.Quote + " "
                paramFile2 = ControlChars.Quote + dirTempStr + "BM_MSCI.QAP" + ControlChars.Quote + " "

                batFile = dirTempStr + "UNIVERSE_MSCI.BAT"
                ConsoleWriteLine(batFile)
                sw = New StreamWriter(batFile)
                sw.WriteLine(mqaExe + mqaUniverseMsciQuery + outputFile + paramFile1 + paramFile2 + "/fq")
                sw.Close()
                sw = Nothing

                Do
                    Shell(batFile, , True)
                    file = New FileInfo(dirTempStr + "UNIVERSE_MSCI_INDEX.CSV")
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
            End If

            ConsoleWriteLine("<<<------ BenchmarksDailyLoad: RunQuery End <<<------")
            ConsoleWriteLine("------>>> BenchmarksDailyLoad: DbLoad Begin ------>>>")

            filesArr = New ArrayList(dirTemp.GetFiles("UNIVERSE_*.CSV"))
            GenerateBcpFiles(filesArr, removeStrings, 1)

            filesArr = New ArrayList(dirTemp.GetFiles("UNIVERSE_*.BCP"))
            batFile = dirTempStr + "BCPBCP.BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            For Each file In filesArr
                sw.WriteLine("bcp QER..universe_makeup_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            Next
            sw.Close()
            sw = Nothing

            sqlQuery = "DELETE universe_makeup_staging"

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

            'NOTE:
            'THIS FOLLOWING PROCEDURE CALL WILL LOAD BASED ON CUSIP ONLY
            'THIS PROGRAM WILL NEED TO BE MODIFIED IF WE BEGIN TO LOAD INTERNATIONAL BENCHMARKS
            sqlQuery = "EXEC universe_makeup_load"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            If bmEqCm Then
                For Each aDate In datesArr
                    sqlQuery = "EXEC universe_makeup_load_from_equity_common @DATE='" + aDate.ToString("d") + "'"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                Next
            End If

            dbConn.Close()

            'NOTE:
            'IF WE EVER DECIDE TO LOAD MSCI BENCHMARKS DAILY WITH THIS SCRIPT, LOAD MSCI_FIF FACTOR HERE
            'IT WILL BE LESS WORK TO DO IT HERE THAN TO LOAD IT IN THE FACTORS DAILY LOAD SCRIPT

            ConsoleWriteLine("<<<------ BenchmarksDailyLoad: DbLoad End <<<------")
            ConsoleWriteLine("------>>> BenchmarksDailyLoad: Archive Begin ------>>>")
            If Not dirArchive.Exists() Then
                dirArchive.Create()
            End If
            ConsoleWriteLine("Archive Directory = " + dirArchiveStr)
            ArchiveTempDir(dirTempStr, dirArchiveStr)
            ConsoleWriteLine("<<<------ BenchmarksDailyLoad: Archive End <<<------")
MainExit:
            ConsoleWriteLine("<<<------ BenchmarksDailyLoad: Main End <<<------")
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
