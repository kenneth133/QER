Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading
Imports Utility

Module LcrDailyLoad

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

        Dim fileDate As DateTime
        If IsDate(System.Configuration.ConfigurationSettings.AppSettings("FileDate")) Then
            fileDate = DateTime.Parse(System.Configuration.ConfigurationSettings.AppSettings("FileDate"))
        Else
            fileDate = Now.Date()
        End If

        Dim rowCountOkay, strategyExists, filesOkay, mqaOutputFiles As Boolean
        Dim i, j As Integer
        Dim dirArchiveStr, dirTempStr, batFile, lineWrite, iStr, jStr As String
        Dim mqaExe, mqaCusipQuery, mqaUniverseQuery, mqaCharQuery, outputFile, paramFile1, paramFile2 As String
        Dim removeStringsMqa As String() = {"NULL", "ERR"}
        Dim removeStringsFs As String() = {"NA", "@NA"}
        Dim prevBusDate As DateTime
        Dim iArr, jArr, factorArr, lineArr As ArrayList
        Dim sr As StreamReader
        Dim sw As StreamWriter
        Dim file As FileInfo
        Dim dir, dirArchive As DirectoryInfo
        Dim fileHash, writerHash, hdrNmIdxHash As Hashtable

        Try
            ConsoleWriteLine("------>>> LcrDailyLoad: Main Begin ------>>>")

            ConsoleWriteLine("Database Server = " + dbServerStr)
            ConsoleWriteLine("Database = " + dbStr)

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn

            sqlQuery = "SELECT * FROM strategy WHERE strategy_cd LIKE 'LCR-%'"
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            strategyExists = rsReader.HasRows()
            rsReader.Close()
            dbConn.Close()

            If Not strategyExists Then
                ConsoleWriteLine("ERROR: No LCR strategies found!")
                ConsoleWriteLine("Terminating program...")
                Exit Try
            End If

            ConsoleWriteLine("------>>> LcrDailyLoad: Initialization Begin ------>>>")

            dbConn.Open()
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
                If rsReader.GetString(0).Equals("ARCHIVE") Then
                    dirArchiveStr = rsReader.GetString(1)
                    dirArchive = New DirectoryInfo(dirArchiveStr)
                ElseIf rsReader.GetString(0).Equals("QALPROC_EXE") Then
                    mqaExe = ControlChars.Quote + rsReader.GetString(1) + ControlChars.Quote + " "
                ElseIf rsReader.GetString(0).Equals("MQA_QUERIES_SPECIAL") Then
                    mqaCusipQuery = ControlChars.Quote + rsReader.GetString(1) + "CUSIP2MQA_ID.QAL" + ControlChars.Quote + " "
                    mqaUniverseQuery = ControlChars.Quote + rsReader.GetString(1) + "UNIVERSE_BY_SECURITY.QAL" + ControlChars.Quote + " "
                    mqaCharQuery = ControlChars.Quote + rsReader.GetString(1) + "CHARACTERISTICS.QAL" + ControlChars.Quote + " "
                End If
            End While
            rsReader.Close()

            sqlQuery = "SELECT d2.decode + d4.decode, d5.decode, d6.decode, u.universe_cd" + ControlChars.NewLine
            sqlQuery += "  FROM decode d1, decode d2, decode d3, decode d4, decode d5, decode d6, strategy g, universe_def u" + ControlChars.NewLine
            sqlQuery += " WHERE d1.item = 'STRATEGY_DIR' AND d1.code LIKE 'LCR-%'" + ControlChars.NewLine
            sqlQuery += "   AND d2.item = 'DIR' AND d2.code = d1.decode" + ControlChars.NewLine
            sqlQuery += "   AND d3.item = 'STRATEGY_FILE' AND d3.code = d1.code" + ControlChars.NewLine
            sqlQuery += "   AND d4.item = 'FILE' AND d4.code = d3.decode" + ControlChars.NewLine
            sqlQuery += "   AND d5.item = 'FILE_HEADER_ROW' AND d5.code = d3.decode" + ControlChars.NewLine
            sqlQuery += "   AND d6.item = 'FILE_DATA_ROW' AND d6.code = d3.decode" + ControlChars.NewLine
            sqlQuery += "   AND d1.code = g.strategy_cd AND g.universe_id = u.universe_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            fileHash = New Hashtable()
            While rsReader.Read()
                iArr = New ArrayList()
                iArr.Add(CInt(rsReader.GetString(1)))
                iArr.Add(CInt(rsReader.GetString(2)))
                iArr.Add(rsReader.GetString(3))
                fileHash.Add(rsReader.GetString(0), iArr)
            End While
            rsReader.Close()

            sqlQuery = "SELECT DISTINCT factor_cd FROM factor"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            factorArr = New ArrayList
            While rsReader.Read()
                factorArr.Add(rsReader.GetString(0))
            End While
            rsReader.Close()

            sqlQuery = "EXEC business_date_get @REF_DATE='" + fileDate.ToString("d") + "', @DIFF=-1, @DATE_FORMAT=101"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                ConsoleWriteLine("File date = " + fileDate.ToString("d"))
                prevBusDate = CDate(rsReader.GetString(0))
                ConsoleWriteLine("Previous business day = " + prevBusDate.ToString("d"))
            End While
            rsReader.Close()

            sqlQuery = "SELECT convert(varchar, getdate(), 112) + '_' + convert(varchar, getdate(), 108)"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                dirTempStr = "C:\temp_Fundamental_LCR_" + rsReader.GetString(0).Replace(":", "") + "\"
            End While
            rsReader.Close()
            dbConn.Close()

            dir = New DirectoryInfo(dirTempStr)
            If Not dir.Exists() Then
                dir.Create()
            End If

            ConsoleWriteLine("<<<------ LcrDailyLoad: Initialization End <<<------")
            ConsoleWriteLine("------>>> LcrDailyLoad: FindFiles Begin ------>>>")

            While True
                filesOkay = True
                For Each iStr In fileHash.Keys()
                    file = New FileInfo(iStr)
                    If file.Exists() Then
                        ConsoleWriteLine("Found " + file.FullName())
                        If file.LastWriteTime().Date().Equals(fileDate.Date()) Then
                            ConsoleWriteLine("Timestamp OK")
                            file.CopyTo(dirTempStr + file.Name())
                        Else
                            ConsoleWriteLine("Timestamp DOES NOT MATCH!")
                            filesOkay = False
                        End If
                    Else
                        ConsoleWriteLine("File " + file.FullName() + " not found!")
                        filesOkay = False
                    End If
                Next

                If filesOkay Then
                    Exit While
                Else
                    ConsoleWriteLine("Sleeping for " + CStr(sysSleepIntervalMinutes) + " minute(s)...")
                    Thread.Sleep(sysSleepIntervalMinutes * sysMinute)
                End If

                If Now() > sleepLimitTime Then
                    ConsoleWriteLine("ERROR: Current time beyond " + sleepLimitTime.TimeOfDay().ToString().Substring(0, 8))
                    ConsoleWriteLine("Terminating program...")
                    Exit Try
                End If
            End While

            ConsoleWriteLine("<<<------ LcrDailyLoad: FindFiles End <<<------")
            ConsoleWriteLine("------>>> LcrDailyLoad: ProcessFiles Begin ------>>>")

            writerHash = New Hashtable()
            For Each iStr In fileHash.Keys()
                file = New FileInfo(iStr)
                iArr = fileHash.Item(iStr)

                If file.Exists() Then
                    ConsoleWriteLine(iStr)
                    hdrNmIdxHash = New Hashtable()
                    sr = file.OpenText()

                    For i = 0 To iArr.Item(0) - 1
                        sr.ReadLine()
                    Next

                    i += 1
                    lineArr = New ArrayList(ChangeDelimiter(sr.ReadLine(), ",", "|").Split("|"))
                    For j = 0 To lineArr.Count() - 1
                        jStr = Trim(lineArr.Item(j))
                        If jStr.StartsWith(ControlChars.Quote) And jStr.EndsWith(ControlChars.Quote) Then
                            jStr = jStr.Substring(1, jStr.Length() - 2)
                        End If
                        jStr = Trim(UCase(jStr))

                        If factorArr.Contains(jStr) Then
                            If Not writerHash.ContainsKey("INSTRUMENT_FACTOR") Then
                                ConsoleWriteLine(dirTempStr + "INSTRUMENT_FACTOR.BCP")
                                sw = New StreamWriter(dirTempStr + "INSTRUMENT_FACTOR.BCP")
                                sw.AutoFlush = True
                                writerHash.Add("INSTRUMENT_FACTOR", sw)
                            End If
                        ElseIf jStr.Equals("CUSIP") Then
                            If Not writerHash.ContainsKey("CUSIP") Then
                                ConsoleWriteLine(dirTempStr + "CUSIP.QAP")
                                sw = New StreamWriter(dirTempStr + "CUSIP.QAP")
                                sw.AutoFlush = True
                                sw.WriteLine("$CUSIP")
                                writerHash.Add("CUSIP", sw)
                            End If
                        End If

                        If jStr.Contains("GIC") And jStr.Contains("SUB") And jStr.Contains("IND") And jStr.Contains("NUM") Then
                            jStr = "GICSSUBINDNUM"
                        ElseIf jStr.Contains("RUSSELL") And jStr.Contains("SECT") And jStr.Contains("NUM") Then
                            jStr = "RUSSELLSECTNUM"
                        ElseIf jStr.Contains("RUSSELL") And jStr.Contains("IND") And jStr.Contains("NUM") Then
                            jStr = "RUSSELLINDNUM"
                        End If

                        hdrNmIdxHash.Add(jStr, j)
                    Next

                    If Not writerHash.ContainsKey("UNIVERSE_FACTSET_STAGING") Then
                        ConsoleWriteLine(dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP")
                        sw = New StreamWriter(dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP")
                        sw.AutoFlush = True
                        writerHash.Add("UNIVERSE_FACTSET_STAGING", sw)
                    End If

                    If Not writerHash.ContainsKey("UNIVERSE_MAKEUP_STAGING") Then
                        ConsoleWriteLine(dirTempStr + "UNIVERSE_MAKEUP_STAGING.BCP")
                        sw = New StreamWriter(dirTempStr + "UNIVERSE_MAKEUP_STAGING.BCP")
                        sw.AutoFlush = True
                        writerHash.Add("UNIVERSE_MAKEUP_STAGING", sw)
                    End If

                    ConsoleWriteLine(dirTempStr + "DATE_" + iArr.Item(2) + ".QAP")
                    sw = New StreamWriter(dirTempStr + "DATE_" + CStr(iArr.Item(2)) + ".QAP")
                    sw.WriteLine("$DATE" + ControlChars.Tab + "$Benchmark_cd")
                    sw.WriteLine(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + iArr.Item(2) + ControlChars.Quote)
                    sw.Close()

                    For i = i To iArr.Item(1) - 1
                        sr.ReadLine()
                    Next

                    ConsoleWrite("Processing")
                    While sr.Peek() >= 0
                        Console.Write(".")
                        lineArr = New ArrayList(ChangeDelimiter(sr.ReadLine(), ",", "|").Split("|"))
                        If lineArr.Count() >= hdrNmIdxHash.Count() Then
                            For j = 0 To lineArr.Count() - 1
                                jStr = Trim(lineArr.Item(j))
                                If jStr.StartsWith(ControlChars.Quote) And jStr.EndsWith(ControlChars.Quote) Then
                                    jStr = jStr.Substring(1, jStr.Length() - 2)
                                End If
                                jStr = Trim(UCase(jStr))
                                If New ArrayList(removeStringsFs).Contains(jStr) Then
                                    jStr = ""
                                End If
                                lineArr.Item(j) = jStr
                            Next

                            sw = writerHash.Item("UNIVERSE_FACTSET_STAGING")
                            lineWrite = prevBusDate.ToString("d") + "|"
                            lineWrite += IdentifiersGet(lineArr, hdrNmIdxHash, "|") + "|"
                            If hdrNmIdxHash.ContainsKey("GICSSUBINDNUM") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("GICSSUBINDNUM"))) Then
                                lineWrite += CStr(CInt(lineArr.Item(hdrNmIdxHash.Item("GICSSUBINDNUM")))) + "|"
                            Else
                                lineWrite += "|"
                            End If
                            If hdrNmIdxHash.ContainsKey("RUSSELLSECTNUM") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("RUSSELLSECTNUM"))) Then
                                lineWrite += CStr(CInt(lineArr.Item(hdrNmIdxHash.Item("RUSSELLSECTNUM")))) + "|"
                            Else
                                lineWrite += "|"
                            End If
                            If hdrNmIdxHash.ContainsKey("RUSSELLINDNUM") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("RUSSELLINDNUM"))) Then
                                lineWrite += CStr(CInt(lineArr.Item(hdrNmIdxHash.Item("RUSSELLINDNUM"))))
                            End If
                            sw.WriteLine(lineWrite)

                            sw = writerHash.Item("UNIVERSE_MAKEUP_STAGING")
                            lineWrite = "|" + prevBusDate.ToString("d") + "|" + iArr.Item(2) + "||"
                            lineWrite += IdentifiersGet(lineArr, hdrNmIdxHash, "|") + "|"
                            sw.WriteLine(lineWrite)

                            If writerHash.ContainsKey("CUSIP") And hdrNmIdxHash.ContainsKey("CUSIP") Then
                                sw = writerHash.Item("CUSIP")
                                lineWrite = ControlChars.Quote + lineArr.Item(hdrNmIdxHash.Item("CUSIP")) + ControlChars.Quote
                                sw.WriteLine(lineWrite)
                            End If

                            For Each jStr In hdrNmIdxHash.Keys()
                                If factorArr.Contains(jStr) Then
                                    sw = writerHash.Item("INSTRUMENT_FACTOR")
                                    lineWrite = prevBusDate.ToString("d") + "|"
                                    lineWrite += IdentifiersGet(lineArr, hdrNmIdxHash, "|") + "|"
                                    lineWrite += jStr + "|"
                                    lineWrite += lineArr.Item(hdrNmIdxHash.Item(jStr))
                                    sw.WriteLine(lineWrite)
                                End If
                            Next
                        End If
                    End While

                    Console.WriteLine("completed")
                    sr.Close()
                Else
                    ConsoleWriteLine("ERROR: " + file.FullName() + " DOES NOT EXIST!")
                    ConsoleWriteLine("Terminating program...")
                    Exit Try
                End If
            Next

            For Each sw In writerHash.Values()
                sw.Close()
            Next

            ConsoleWriteLine(dirTempStr + "DATE.QAP")
            sw = New StreamWriter(dirTempStr + "DATE.QAP")
            sw.WriteLine("$DATE")
            sw.WriteLine(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote)
            sw.Close()

            ConsoleWriteLine(dirTempStr + "CHAR_DATE.QAP")
            sw = New StreamWriter(dirTempStr + "CHAR_DATE.QAP")
            sw.WriteLine("$StartDate" + ControlChars.Tab + "$EndDate")
            sw.WriteLine(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + prevBusDate + ControlChars.Quote)
            sw.Close()

            ConsoleWriteLine("<<<------ LcrDailyLoad: ProcessFiles End <<<------")
            ConsoleWriteLine("------>>> LcrDailyLoad: StagingTablesLoad Begin ------>>>")

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
                    ConsoleWriteLine("MQA output file " + file.Name() + " not found!")
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

            iArr = New ArrayList(dir.GetFiles("CusipMqaId*.CSV"))
            GenerateBcpFiles(iArr, removeStringsMqa, 1)

            batFile = dirTempStr + "bcp01.BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            sw.WriteLine("bcp QER..cusip2mqa_id_staging in " + dirTempStr + "CusipMqaId.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            sqlQuery = "DELETE cusip2mqa_id_staging"
            file = New FileInfo(dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP")
            If file.Exists() Then
                sw.WriteLine("bcp QER..universe_factset_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sqlQuery += ControlChars.NewLine + "DELETE universe_factset_staging"
            End If
            file = New FileInfo(dirTempStr + "UNIVERSE_MAKEUP_STAGING.BCP")
            If file.Exists() Then
                sw.WriteLine("bcp QER..universe_makeup_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sqlQuery += ControlChars.NewLine + "DELETE universe_makeup_staging"
            End If
            file = New FileInfo(dirTempStr + "INSTRUMENT_FACTOR.BCP")
            If file.Exists() Then
                sw.WriteLine("bcp QER..instrument_factor_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sqlQuery += ControlChars.NewLine + "DELETE instrument_factor_staging"
            End If
            sw.Close()
            sw = Nothing

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
                    ConsoleWriteLine("Terminating program...")
                    Exit Try
                End If

                bcpAttemptCount += 1
            Loop While Not rowCountOkay

            sqlQuery = "SELECT DISTINCT * INTO #cusip2mqa_id_staging FROM cusip2mqa_id_staging" + ControlChars.NewLine
            sqlQuery += "DELETE cusip2mqa_id_staging" + ControlChars.NewLine
            sqlQuery += "INSERT cusip2mqa_id_staging SELECT * FROM #cusip2mqa_id_staging" + ControlChars.NewLine
            sqlQuery += "DROP TABLE #cusip2mqa_id_staging"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            sqlQuery = "SELECT DISTINCT * INTO #universe_factset_staging FROM universe_factset_staging" + ControlChars.NewLine
            sqlQuery += "DELETE universe_factset_staging" + ControlChars.NewLine
            sqlQuery += "INSERT universe_factset_staging SELECT * FROM #universe_factset_staging" + ControlChars.NewLine
            sqlQuery += "DROP TABLE #universe_factset_staging"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            sqlQuery = "SELECT DISTINCT u.universe_cd, c.mqa_id" + ControlChars.NewLine
            sqlQuery += "  FROM cusip2mqa_id_staging c, universe_makeup_staging u" + ControlChars.NewLine
            sqlQuery += " WHERE c.input_cusip = u.cusip" + ControlChars.NewLine
            sqlQuery += "   AND c.mqa_id IS NOT NULL" + ControlChars.NewLine
            sqlQuery += " ORDER BY u.universe_cd, c.mqa_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            iStr = ""
            While rsReader.Read()
                If iStr.Equals(rsReader.GetString(0)) Then
                    sw.WriteLine(ControlChars.Quote + rsReader.GetString(1) + ControlChars.Quote)
                Else
                    If Not sw Is Nothing Then
                        sw.Close()
                        sw = Nothing
                    End If

                    iStr = rsReader.GetString(0)
                    ConsoleWriteLine(dirTempStr + "MQA_ID_" + iStr + ".QAP")
                    sw = New StreamWriter(dirTempStr + "MQA_ID_" + iStr + ".QAP")
                    sw.WriteLine("$ID")
                    sw.WriteLine(ControlChars.Quote + rsReader.GetString(1) + ControlChars.Quote)
                End If
            End While
            sw.Close()
            rsReader.Close()

            sqlQuery = "SELECT DISTINCT mqa_id FROM cusip2mqa_id_staging" + ControlChars.NewLine
            sqlQuery += " WHERE mqa_id IS NOT NULL" + ControlChars.NewLine
            sqlQuery += "   AND mqa_id NOT IN (SELECT mqa_id FROM instrument_characteristics" + ControlChars.NewLine
            sqlQuery += "                       WHERE bdate = '" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
            sqlQuery += "                         AND mqa_id IS NOT NULL)" + ControlChars.NewLine
            sqlQuery += " ORDER BY mqa_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            If rsReader.HasRows() Then
                ConsoleWriteLine(dirTempStr + "CHAR_MQA_ID.QAP")
                sw = New StreamWriter(dirTempStr + "CHAR_MQA_ID.QAP")
                sw.WriteLine("$ID")
                While rsReader.Read()
                    sw.WriteLine(ControlChars.Quote + rsReader.GetString(0) + ControlChars.Quote)
                End While
                sw.Close()
            End If
            rsReader.Close()
            dbConn.Close()

            batFile = dirTempStr + "MQA_QUERIES.BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            For Each iStr In fileHash.Keys()
                iArr = fileHash.Item(iStr)
                file = New FileInfo(dirTempStr + "DATE_" + iArr.Item(2) + ".QAP")
                If Not file.Exists() Then
                    ConsoleWriteLine("ERROR: File " + dirTempStr + "DATE_" + iArr.Item(2) + ".QAP not found!")
                    ConsoleWriteLine("Terminating program...")
                    Exit Try
                End If
                file = New FileInfo(dirTempStr + "MQA_ID_" + iArr.Item(2) + ".QAP")
                If Not file.Exists() Then
                    ConsoleWriteLine("ERROR: File " + dirTempStr + "DATE_" + iArr.Item(2) + ".QAP not found!")
                    ConsoleWriteLine("Terminating program...")
                    Exit Try
                End If

                outputFile = ControlChars.Quote + dirTempStr + "UNIVERSE_" + iArr.Item(2) + ".CSV" + ControlChars.Quote + " "
                paramFile1 = ControlChars.Quote + dirTempStr + "MQA_ID_" + iArr.Item(2) + ".QAP" + ControlChars.Quote + " "
                paramFile2 = ControlChars.Quote + dirTempStr + "DATE_" + iArr.Item(2) + ".QAP" + ControlChars.Quote + " "
                sw.WriteLine(mqaExe + mqaUniverseQuery + outputFile + paramFile1 + paramFile2 + "/fc")
            Next

            outputFile = ControlChars.Quote + dirTempStr + "CHARACTERISTICS.CSV" + ControlChars.Quote + " "
            paramFile1 = ControlChars.Quote + dirTempStr + "CHAR_MQA_ID.QAP" + ControlChars.Quote + " "
            paramFile2 = ControlChars.Quote + dirTempStr + "CHAR_DATE.QAP" + ControlChars.Quote + " "
            sw.WriteLine(mqaExe + mqaCharQuery + outputFile + paramFile1 + paramFile2 + "/fc")
            sw.Close()

            Do
                Shell(batFile, , True)
                mqaOutputFiles = True
                sr = New StreamReader(batFile)
                While sr.Peek() >= 0
                    lineArr = New ArrayList(sr.ReadLine().Split(" "))
                    file = New FileInfo(CStr(lineArr.Item(2)).Replace(ControlChars.Quote, ""))
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

            iArr = New ArrayList(dir.GetFiles("UNIVERSE_LCR-*.CSV"))
            iArr.AddRange(dir.GetFiles("CHARACTERISTICS*.CSV"))
            GenerateBcpFiles(iArr, removeStringsMqa, 1)

            iArr = New ArrayList(dir.GetFiles("UNIVERSE_LCR-*.BCP"))
            For Each file In iArr
                sr = New StreamReader(file.FullName())
                sw = New StreamWriter(CStr(file.FullName().Split(".").GetValue(0)) + "2.BCP")
                While sr.Peek() >= 0
                    lineArr = New ArrayList(sr.ReadLine().Split("|"))
                    sw.Write(lineArr.Item(3))
                    For i = 0 To lineArr.Count() - 1
                        sw.Write("|" + lineArr.Item(i))
                    Next
                    sw.WriteLine()
                End While
                sw.Close()
                sr.Close()
            Next

            batFile = dirTempStr + "bcp02.BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            iArr = New ArrayList(dir.GetFiles("UNIVERSE_LCR-*2.BCP"))
            iArr.AddRange(dir.GetFiles("CHARACTERISTICS*.BCP"))
            For Each file In iArr
                If file.Name().Contains("UNIVERSE_LCR") Then
                    sw.WriteLine("bcp QER..universe_makeup_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                ElseIf file.Name().Contains("CHARACTERISTICS") Then
                    sw.WriteLine("bcp QER..instrument_characteristics_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                End If
            Next
            sw.Close()

            sqlQuery = "DELETE universe_makeup_staging" + ControlChars.NewLine
            sqlQuery += "DELETE instrument_characteristics_staging"

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
                    ConsoleWriteLine("Terminating program...")
                    Exit Try
                End If

                bcpAttemptCount += 1
            Loop While Not rowCountOkay

            sqlQuery = "UPDATE universe_factset_staging" + ControlChars.NewLine
            sqlQuery += "   SET mqa_id = c.mqa_id" + ControlChars.NewLine
            sqlQuery += "  FROM cusip2mqa_id_staging c" + ControlChars.NewLine
            sqlQuery += " WHERE universe_factset_staging.universe_dt = c.bdate" + ControlChars.NewLine
            sqlQuery += "   AND universe_factset_staging.cusip = c.input_cusip"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")

            sqlQuery = "INSERT instrument_characteristics_staging" + ControlChars.NewLine
            sqlQuery += "      (bdate, ticker, cusip, sedol, isin, gv_key," + ControlChars.NewLine
            sqlQuery += "       gics_sector_num, gics_segment_num, gics_industry_num, gics_sub_industry_num," + ControlChars.NewLine
            sqlQuery += "       russell_sector_num, russell_industry_num)" + ControlChars.NewLine
            sqlQuery += "SELECT universe_dt, ticker, cusip, sedol, isin, gv_key," + ControlChars.NewLine
            sqlQuery += "       substring(convert(varchar, gics_sub_industry_num), 1, 2)," + ControlChars.NewLine
            sqlQuery += "       substring(convert(varchar, gics_sub_industry_num), 1, 4)," + ControlChars.NewLine
            sqlQuery += "       substring(convert(varchar, gics_sub_industry_num), 1, 6)," + ControlChars.NewLine
            sqlQuery += "       gics_sub_industry_num," + ControlChars.NewLine
            sqlQuery += "       russell_sector_num, russell_industry_num" + ControlChars.NewLine
            sqlQuery += "  FROM universe_factset_staging" + ControlChars.NewLine
            sqlQuery += " WHERE mqa_id IS NULL" + ControlChars.NewLine
            sqlQuery += "    OR cusip NOT IN (SELECT cusip FROM instrument_characteristics_staging WHERE cusip IS NOT NULL" + ControlChars.NewLine
            sqlQuery += "                     UNION" + ControlChars.NewLine
            sqlQuery += "                     SELECT cusip FROM instrument_characteristics WHERE bdate='" + prevBusDate.ToString("d") + "' AND cusip IS NOT NULL)"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")

            iArr = New ArrayList()
            iArr.Add("universe_factset_staging")
            iArr.Add("universe_makeup_staging")
            iArr.Add("instrument_characteristics_staging")

            ConsoleWriteLine()
            For Each iStr In iArr
                sqlQuery = "SELECT COUNT(*) FROM " + iStr
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                While rsReader.Read()
                    Console.WriteLine(CStr(rsReader.GetInt32(0)))
                End While
                rsReader.Close()
            Next

            iArr = New ArrayList()
            iArr.Add("sedol")
            iArr.Add("isin")
            iArr.Add("gv_key")

            jArr = New ArrayList()
            jArr.Add("instrument_characteristics_staging")
            jArr.Add("instrument_characteristics")

            For Each jStr In jArr
                For Each iStr In iArr
                    sqlQuery = "UPDATE " + jStr + ControlChars.NewLine
                    sqlQuery += "   SET " + iStr + " = f." + iStr + ControlChars.NewLine
                    sqlQuery += "  FROM cusip2mqa_id_staging c, universe_factset_staging f" + ControlChars.NewLine
                    sqlQuery += " WHERE f.universe_dt = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND f.cusip = c.input_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + ".bdate = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + ".cusip = c.mqa_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + "." + iStr + " IS NULL"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")
                Next
            Next

            For Each jStr In jArr
                For Each iStr In iArr
                    sqlQuery = "UPDATE universe_makeup_staging" + ControlChars.NewLine
                    sqlQuery += "   SET " + iStr + " = i." + iStr + ControlChars.NewLine
                    sqlQuery += "  FROM cusip2mqa_id_staging c, " + jStr + " i" + ControlChars.NewLine
                    sqlQuery += " WHERE i.bdate = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND i.cusip = c.mqa_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND universe_makeup_staging.universe_dt = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND universe_makeup_staging.cusip = c.input_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND universe_makeup_staging." + iStr + " IS NULL"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")
                Next
            Next

            iArr = New ArrayList()
            iArr.Add("mqa_id")
            iArr.Add("ticker")
            iArr.Add("sedol")
            iArr.Add("isin")
            iArr.Add("gv_key")

            For Each iStr In iArr
                sqlQuery = "UPDATE instrument_factor_staging" + ControlChars.NewLine
                sqlQuery += "   SET " + iStr + " = p." + iStr + ControlChars.NewLine
                sqlQuery += "  FROM cusip2mqa_id_staging c, universe_makeup_staging p" + ControlChars.NewLine
                sqlQuery += " WHERE p.universe_dt = c.bdate" + ControlChars.NewLine
                sqlQuery += "   AND p.cusip = c.mqa_cusip" + ControlChars.NewLine
                sqlQuery += "   AND instrument_factor_staging.bdate = c.bdate" + ControlChars.NewLine
                sqlQuery += "   AND instrument_factor_staging.cusip = c.input_cusip" + ControlChars.NewLine
                sqlQuery += "   AND instrument_factor_staging." + iStr + " IS NULL"
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")
            Next

            iArr = New ArrayList()
            iArr.Add("gics_sub_industry_num")
            iArr.Add("russell_sector_num")
            iArr.Add("russell_industry_num")

            jArr = New ArrayList()
            jArr.Add("instrument_characteristics_staging")
            jArr.Add("instrument_characteristics")

            For Each jStr In jArr
                For Each iStr In iArr
                    sqlQuery = "UPDATE universe_factset_staging" + ControlChars.NewLine
                    sqlQuery += "   SET " + iStr + " = i." + iStr + ControlChars.NewLine
                    sqlQuery += "  FROM cusip2mqa_id_staging c, " + jStr + " i" + ControlChars.NewLine
                    sqlQuery += " WHERE c.bdate = i.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND c.mqa_cusip = i.cusip" + ControlChars.NewLine
                    sqlQuery += "   AND universe_factset_staging.universe_dt = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND universe_factset_staging.cusip = c.input_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND universe_factset_staging." + iStr + " IS NULL"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")
                Next
            Next

            For Each jStr In jArr
                For Each iStr In iArr
                    sqlQuery = "UPDATE " + jStr + ControlChars.NewLine
                    sqlQuery += "   SET " + iStr + " = f." + iStr + ControlChars.NewLine
                    sqlQuery += "  FROM cusip2mqa_id_staging c, universe_factset_staging f" + ControlChars.NewLine
                    sqlQuery += " WHERE f.universe_dt = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND f.cusip = c.input_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + ".bdate = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + ".cusip = c.mqa_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + "." + iStr + " IS NULL"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")
                Next
            Next

            ConsoleWriteLine("<<<------ LcrDailyLoad: StagingTablesLoad End <<<------")
            ConsoleWriteLine("------>>> LcrDailyLoad: PermanentTablesLoad Begin ------>>>")

            sqlQuery = "EXEC universe_makeup_load"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            sqlQuery = "EXEC instrument_characteristics_load"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            sqlQuery = "SELECT i.ticker, i.cusip, i.sedol, i.isin," + ControlChars.NewLine
            sqlQuery += "       i.russell_sector_num, f.russell_sector_num," + ControlChars.NewLine
            sqlQuery += "       i.russell_industry_num, f.russell_industry_num," + ControlChars.NewLine
            sqlQuery += "       i.gics_sub_industry_num, f.gics_sub_industry_num" + ControlChars.NewLine
            sqlQuery += "  FROM instrument_characteristics i, universe_factset_staging f, cusip2mqa_id_staging c" + ControlChars.NewLine
            sqlQuery += " WHERE f.universe_dt = c.bdate" + ControlChars.NewLine
            sqlQuery += "   AND f.cusip = c.input_cusip" + ControlChars.NewLine
            sqlQuery += "   AND i.bdate = c.bdate" + ControlChars.NewLine
            sqlQuery += "   AND i.cusip = c.mqa_cusip" + ControlChars.NewLine
            sqlQuery += "   AND (i.gics_sub_industry_num != f.gics_sub_industry_num OR" + ControlChars.NewLine
            sqlQuery += "        i.russell_sector_num != f.russell_sector_num OR" + ControlChars.NewLine
            sqlQuery += "        i.russell_industry_num != f.russell_industry_num)"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            Console.WriteLine("GICS RUSSELL DATA CHECK")
            Console.WriteLine("--------------------------------------------------------------")
            While rsReader.Read()
                If rsReader.IsDBNull(0) Then
                    Console.Write("NULL ")
                Else
                    Console.Write(rsReader.GetString(0) + " ")
                End If
                If rsReader.IsDBNull(1) Then
                    Console.Write("NULL ")
                Else
                    Console.Write(rsReader.GetString(1) + " ")
                End If
                If rsReader.IsDBNull(2) Then
                    Console.Write("NULL ")
                Else
                    Console.Write(rsReader.GetString(2) + " ")
                End If
                If rsReader.IsDBNull(3) Then
                    Console.WriteLine("NULL")
                Else
                    Console.WriteLine(rsReader.GetString(3))
                End If

                If rsReader.IsDBNull(4) Then
                    Console.Write("NULL ")
                Else
                    Console.Write(CStr(rsReader.GetInt32(4)) + " ")
                End If
                If rsReader.IsDBNull(5) Then
                    Console.Write("NULL    ")
                Else
                    Console.Write(CStr(rsReader.GetInt32(5)) + "    ")
                End If

                If rsReader.IsDBNull(6) Then
                    Console.Write("NULL ")
                Else
                    Console.Write(CStr(rsReader.GetInt32(6)) + " ")
                End If
                If rsReader.IsDBNull(7) Then
                    Console.Write("NULL    ")
                Else
                    Console.Write(CStr(rsReader.GetInt32(7)) + "    ")
                End If

                If rsReader.IsDBNull(8) Then
                    Console.Write("NULL ")
                Else
                    Console.Write(CStr(rsReader.GetInt32(8)) + " ")
                End If
                If rsReader.IsDBNull(9) Then
                    Console.WriteLine("NULL")
                Else
                    Console.WriteLine(CStr(rsReader.GetInt32(9)))
                End If
            End While
            Console.WriteLine("--------------------------------------------------------------")
            rsReader.Close()

            sqlQuery = "EXEC instrument_factor_load @SOURCE_CD='FS'"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            For Each iStr In fileHash.Keys()
                iArr = fileHash.Item(iStr)
                sqlQuery = "EXEC universe_makeup_weight_update @UNIVERSE_DT='" + prevBusDate.ToString("d") + "', @UNIVERSE_CD='" + iArr.Item(2) + "'"
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()
            Next

            sqlQuery = "SELECT g.universe_id, m.sector_model_id" + ControlChars.NewLine
            sqlQuery += "  FROM strategy g, factor_model m" + ControlChars.NewLine
            sqlQuery += " WHERE g.strategy_cd LIKE 'LCR-%'" + ControlChars.NewLine
            sqlQuery += "   AND g.factor_model_id = m.factor_model_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            sqlQuery = ""
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                sqlQuery += "EXEC sector_model_security_populate @BDATE='" + prevBusDate.ToString("d") + "', @UNIVERSE_DT='" + prevBusDate.ToString("d") + "', @UNIVERSE_ID=" + CStr(rsReader.GetInt32(0)) + ", @SECTOR_MODEL_ID=" + CStr(rsReader.GetInt32(1)) + ControlChars.NewLine
            End While
            rsReader.Close()

            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            sqlQuery = "SELECT d.sector_model_id, d.sector_id" + ControlChars.NewLine
            sqlQuery += "  FROM strategy g, factor_model m, sector_def d" + ControlChars.NewLine
            sqlQuery += " WHERE g.strategy_cd like 'LCR-%'" + ControlChars.NewLine
            sqlQuery += "   AND g.factor_model_id = m.factor_model_id" + ControlChars.NewLine
            sqlQuery += "   AND m.sector_model_id = d.sector_model_id" + ControlChars.NewLine
            sqlQuery += "   AND NOT EXISTS (SELECT p.* FROM sector_makeup p" + ControlChars.NewLine
            sqlQuery += "                    WHERE p.sector_id = d.sector_id)"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            sqlQuery = ""
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                sqlQuery += "UPDATE sector_model_security SET sector_id=" + CStr(rsReader.GetInt32(1)) + " WHERE sector_model_id=" + CStr(rsReader.GetInt32(0)) + " AND bdate='" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
            End While
            rsReader.Close()

            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            ConsoleWriteLine("<<<------ LcrDailyLoad: PermanentTablesLoad End <<<------")
            ConsoleWriteLine("------>>> LcrDailyLoad: RunRanksScores Begin ------>>>")

            sqlQuery = "SELECT strategy_id FROM strategy" + ControlChars.NewLine
            sqlQuery += " WHERE strategy_cd LIKE 'LCR-%' ORDER BY strategy_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            jArr = New ArrayList
            While rsReader.Read()
                jArr.Add(rsReader.GetInt32(0))
            End While
            rsReader.Close()

            iArr = New ArrayList()
            iArr.Add("strategy_ranks_run")
            iArr.Add("strategy_scores_compute")
            iArr.Add("model_portfolio_populate")

            For Each i In jArr
                For Each iStr In iArr
                    sqlQuery = "EXEC " + iStr + " @BDATE='" + prevBusDate.ToString("d") + "', @STRATEGY_ID=" + CStr(i)
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                Next
            Next

            dbConn.Close()
            ConsoleWriteLine("<<<------ LcrDailyLoad: RunRanksScores End <<<------")
            ConsoleWriteLine("------>>> LcrDailyLoad: Archive Begin ------>>>")
            If Not dirArchive.Exists() Then
                dirArchive.Create()
            End If
            ConsoleWriteLine("Archive Directory = " + dirArchiveStr)
            ArchiveTempDir(dirTempStr, dirArchiveStr)
            ConsoleWriteLine("<<<------ LcrDailyLoad: Archive End <<<------")
            ConsoleWriteLine("<<<------ LcrDailyLoad: Main End <<<------")
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
