Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading
Imports Utility

Module ModelRanksDailyLoad

    Sub Main(ByVal args() As String)
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

        Dim rowCountOkay, strategyExists As Boolean
        Dim i, j, fractile, bcpBatCount As Integer
        Dim dirArchiveStr, dirTempStr, batFile, strategy, universeCd, lineWrite, iStr, jStr As String
        Dim mqaExe, mqaCusipQuery, mqaUniverseQuery, mqaCharQuery, outputFile, paramFile1, paramFile2 As String
        Dim removeStringsMqa As String() = {"NULL", "ERR"}
        Dim removeStringsFs As String() = {"NA", "@NA"}
        Dim prevBusDate As DateTime
        Dim iArr, jArr, factorArr, categoryArr, lineArr As ArrayList
        Dim sr As StreamReader
        Dim sw As StreamWriter
        Dim file As FileInfo
        Dim dir, dirArchive As DirectoryInfo
        Dim fileHash, writerHash, hdrNmIdxHash As Hashtable

        Try
            ConsoleWriteLine("------>>> ModelRanksDailyLoad: Main Begin ------>>>")

            If args.Length() < 1 Then
                Console.Write("Strategy? ")
                strategy = Console.ReadLine().ToUpper()
            Else
                strategy = CStr(args.GetValue(0)).ToUpper()
            End If

            ConsoleWriteLine("Database Server = " + dbServerStr)
            ConsoleWriteLine("Database = " + dbStr)
            ConsoleWriteLine("Strategy = " + strategy)

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn

            sqlQuery = "SELECT * FROM strategy WHERE strategy_cd = '" + strategy + "'"
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            strategyExists = rsReader.HasRows()
            rsReader.Close()
            dbConn.Close()

            If Not strategyExists Then
                ConsoleWriteLine("ERROR: Strategy '" + strategy + "' not found!")
                ConsoleWriteLine("Terminating program...")
                Exit Try
            End If

            ConsoleWriteLine("------>>> ModelRanksDailyLoad: Initialization Begin ------>>>")

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

            sqlQuery = "SELECT d2.decode + d4.decode, d5.decode, d6.decode" + ControlChars.NewLine
            sqlQuery += "  FROM decode d1, decode d2, decode d3, decode d4, decode d5, decode d6" + ControlChars.NewLine
            sqlQuery += " WHERE d1.item = 'STRATEGY_DIR' AND d1.code = '" + strategy + "'" + ControlChars.NewLine
            sqlQuery += "   AND d2.item = 'DIR' AND d2.code = d1.decode" + ControlChars.NewLine
            sqlQuery += "   AND d3.item = 'STRATEGY_FILE' AND d3.code = d1.code" + ControlChars.NewLine
            sqlQuery += "   AND d4.item = 'FILE' AND d4.code = d3.decode" + ControlChars.NewLine
            sqlQuery += "   AND d5.item = 'FILE_HEADER_ROW' AND d5.code = d3.decode" + ControlChars.NewLine
            sqlQuery += "   AND d6.item = 'FILE_DATA_ROW' AND d6.code = d3.decode"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            fileHash = New Hashtable()
            While rsReader.Read()
                iArr = New ArrayList()
                iArr.Add(CInt(rsReader.GetString(1)))
                iArr.Add(CInt(rsReader.GetString(2)))
                fileHash.Add(rsReader.GetString(0), iArr)
            End While
            rsReader.Close()

            sqlQuery = "SELECT d.universe_cd, g.fractile" + ControlChars.NewLine
            sqlQuery += "  FROM strategy g, universe_def d" + ControlChars.NewLine
            sqlQuery += " WHERE g.strategy_cd = '" + strategy + "'" + ControlChars.NewLine
            sqlQuery += "   AND g.universe_id = d.universe_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                universeCd = rsReader.GetString(0)
                fractile = rsReader.GetInt32(1)
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

            sqlQuery = "SELECT DISTINCT decode FROM decode WHERE item = 'FACTOR_CATEGORY'"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            categoryArr = New ArrayList
            While rsReader.Read()
                categoryArr.Add(rsReader.GetString(0).Replace(" ", "").Replace("_", "").Replace("-", ""))
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
                dirTempStr = "C:\temp_ModelRanksDailyLoad_" + strategy + "_" + rsReader.GetString(0).Replace(":", "") + "\"
            End While
            rsReader.Close()
            dbConn.Close()

            dir = New DirectoryInfo(dirTempStr)
            If Not dir.Exists() Then
                dir.Create()
            End If

            ConsoleWriteLine("<<<------ ModelRanksDailyLoad: Initialization End <<<------")
            ConsoleWriteLine("------>>> ModelRanksDailyLoad: FindFile Begin ------>>>")

            For Each iStr In fileHash.Keys()
                While True
                    file = New FileInfo(iStr)
                    If file.Exists() Then
                        ConsoleWriteLine("Found " + file.FullName())
                        GoTo filefound
                    End If

                    ConsoleWriteLine("File " + file.FullName() + " not found!")
                    GoTo FileNotFound
FileFound:
                    If file.LastWriteTime().Date().Equals(fileDate.Date()) Then
                        ConsoleWriteLine("Timestamp OK")
                        file.CopyTo(dirTempStr + file.Name())
                        Exit While
                    Else
                        ConsoleWriteLine("Timestamp DOES NOT MATCH!")
                    End If
FileNotFound:
                    ConsoleWriteLine("Sleeping for " + CStr(sysSleepIntervalMinutes) + " minute(s)...")
                    Thread.Sleep(sysSleepIntervalMinutes * sysMinute)

                    If Now() > sleepLimitTime Then
                        ConsoleWriteLine("ERROR: Current time beyond " + sleepLimitTime.TimeOfDay().ToString().Substring(0, 8))
                        ConsoleWriteLine("Terminating program...")
                        Exit Try
                    End If
                End While
            Next

            ConsoleWriteLine("<<<------ ModelRanksDailyLoad: FindFile End <<<------")
            ConsoleWriteLine("------>>> ModelRanksDailyLoad: ProcessFile Begin ------>>>")

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

                        jArr = New ArrayList(jStr.Split(" "))
                        If factorArr.Contains(jArr.Item(0)) And jArr.Count() > 1 Then
                            jArr.Item(1) = CStr(jArr.Item(1)).Replace("_", "").Replace("-", "")
                            If jArr.Item(1).Equals("RAW") Then
                                If Not writerHash.ContainsKey("INSTRUMENT_FACTOR") Then
                                    ConsoleWriteLine(dirTempStr + "INSTRUMENT_FACTOR.BCP")
                                    sw = New StreamWriter(dirTempStr + "INSTRUMENT_FACTOR.BCP")
                                    sw.AutoFlush = True
                                    writerHash.Add("INSTRUMENT_FACTOR", sw)
                                End If
                            ElseIf jArr.Item(1).Equals("GLOBALRANK") _
                            Or jArr.Item(1).Equals("UNIVERSERANK") _
                            Or jArr.Item(1).Equals("SECTORRANK") _
                            Or jArr.Item(1).Equals("SEGMENTRANK") Then
                                If Not writerHash.ContainsKey(CStr(jArr.Item(0)) + " " + CStr(jArr.Item(1))) Then
                                    ConsoleWriteLine(dirTempStr + CStr(jArr.Item(0)) + "_" + CStr(jArr.Item(1)))
                                    sw = New StreamWriter(dirTempStr + CStr(jArr.Item(0)) + "_" + CStr(jArr.Item(1)) + ".BCP")
                                    sw.AutoFlush = True
                                    writerHash.Add(CStr(jArr.Item(0)) + " " + CStr(jArr.Item(1)), sw)
                                End If
                            End If
                        Else
                            jArr.Item(0) = CStr(jArr.Item(0)).Replace("_", "").Replace("-", "")
                            If categoryArr.Contains(jArr.Item(0)) Then
                                If Not writerHash.ContainsKey("CATEGORY_SCORE") Then
                                    ConsoleWriteLine(dirTempStr + "CATEGORY_SCORE.BCP")
                                    sw = New StreamWriter(dirTempStr + "CATEGORY_SCORE.BCP")
                                    sw.AutoFlush = True
                                    writerHash.Add("CATEGORY_SCORE", sw)
                                End If
                            ElseIf jArr.Item(0).Equals("TOTALSCORE") _
                            Or jArr.Item(0).Equals("UNIVERSESCORE") _
                            Or jArr.Item(0).Equals("GLOBALSCORE") _
                            Or jArr.Item(0).Equals("SECTORSCORE") _
                            Or jArr.Item(0).Equals("SEGMENTSCORE") _
                            Or jArr.Item(0).Equals("SSSCORE") _
                            Or jArr.Item(0).Equals("COUNTRYSCORE") Then
                                If Not writerHash.ContainsKey("SCORES") Then
                                    ConsoleWriteLine(dirTempStr + "SCORES.BCP")
                                    sw = New StreamWriter(dirTempStr + "SCORES.BCP")
                                    sw.AutoFlush = True
                                    writerHash.Add("SCORES", sw)
                                End If
                            ElseIf jArr.Item(0).Equals("CUSIP") Then
                                If Not writerHash.ContainsKey("CUSIP") Then
                                    ConsoleWriteLine(dirTempStr + "CUSIP.QAP")
                                    sw = New StreamWriter(dirTempStr + "CUSIP.QAP")
                                    sw.AutoFlush = True
                                    sw.WriteLine("$CUSIP")
                                    writerHash.Add("CUSIP", sw)
                                End If
                            End If
                        End If

                        If CStr(jArr.Item(0)).Contains("GIC") And CStr(jArr.Item(0)).Contains("SUB") And CStr(jArr.Item(0)).Contains("IND") And CStr(jArr.Item(0)).Contains("NUM") Then
                            jArr.Item(0) = "GICSSUBINDNUM"
                        ElseIf CStr(jArr.Item(0)).Contains("RUSSELL") And CStr(jArr.Item(0)).Contains("SECT") And CStr(jArr.Item(0)).Contains("NUM") Then
                            jArr.Item(0) = "RUSSELLSECTNUM"
                        ElseIf CStr(jArr.Item(0)).Contains("RUSSELL") And CStr(jArr.Item(0)).Contains("IND") And CStr(jArr.Item(0)).Contains("NUM") Then
                            jArr.Item(0) = "RUSSELLINDNUM"
                        End If

                        jStr = jArr.Item(0)
                        If jArr.Count() > 1 Then
                            jStr += " " + jArr.Item(1)
                        End If
                        hdrNmIdxHash.Add(jStr, j)
                    Next

                    If Not writerHash.ContainsKey("UNIVERSE_FACTSET_STAGING") Then
                        ConsoleWriteLine(dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP")
                        sw = New StreamWriter(dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP")
                        sw.AutoFlush = True
                        writerHash.Add("UNIVERSE_FACTSET_STAGING", sw)
                    End If

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

                            If writerHash.ContainsKey("CUSIP") And hdrNmIdxHash.ContainsKey("CUSIP") Then
                                sw = writerHash.Item("CUSIP")
                                lineWrite = ControlChars.Quote + lineArr.Item(hdrNmIdxHash.Item("CUSIP")) + ControlChars.Quote
                                sw.WriteLine(lineWrite)
                            End If

                            If writerHash.ContainsKey("SCORES") Then
                                sw = writerHash.Item("SCORES")
                                lineWrite = prevBusDate.ToString("d") + "|" + strategy + "|"
                                lineWrite += IdentifiersGet(lineArr, hdrNmIdxHash, "|") + "|"
                                If hdrNmIdxHash.ContainsKey("SECTORSCORE") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("SECTORSCORE"))) Then
                                    lineWrite += lineArr.Item(hdrNmIdxHash.Item("SECTORSCORE")) + "|"
                                Else
                                    lineWrite += "|"
                                End If
                                If hdrNmIdxHash.ContainsKey("SEGMENTSCORE") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("SEGMENTSCORE"))) Then
                                    lineWrite += lineArr.Item(hdrNmIdxHash.Item("SEGMENTSCORE")) + "|"
                                Else
                                    lineWrite += "|"
                                End If
                                If hdrNmIdxHash.ContainsKey("SSSCORE") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("SSSCORE"))) Then
                                    lineWrite += lineArr.Item(hdrNmIdxHash.Item("SSSCORE")) + "|"
                                Else
                                    lineWrite += "|"
                                End If
                                If (hdrNmIdxHash.ContainsKey("UNIVERSESCORE") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("UNIVERSESCORE")))) _
                                Or (hdrNmIdxHash.ContainsKey("GLOBALSCORE") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("GLOBALSCORE")))) Then
                                    If hdrNmIdxHash.ContainsKey("UNIVERSESCORE") Then
                                        lineWrite += lineArr.Item(hdrNmIdxHash.Item("UNIVERSESCORE")) + "|"
                                    ElseIf hdrNmIdxHash.ContainsKey("GLOBALSCORE") Then
                                        lineWrite += lineArr.Item(hdrNmIdxHash.Item("GLOBALSCORE")) + "|"
                                    End If
                                Else
                                    lineWrite += "|"
                                End If
                                If hdrNmIdxHash.ContainsKey("COUNTRYSCORE") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("COUNTRYSCORE"))) Then
                                    lineWrite += lineArr.Item(hdrNmIdxHash.Item("COUNTRYSCORE")) + "|"
                                Else
                                    lineWrite += "|"
                                End If
                                If hdrNmIdxHash.ContainsKey("TOTALSCORE") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("TOTALSCORE"))) Then
                                    lineWrite += lineArr.Item(hdrNmIdxHash.Item("TOTALSCORE"))
                                End If
                                sw.WriteLine(lineWrite)
                            End If

                            For Each jStr In hdrNmIdxHash.Keys()
                                jArr = New ArrayList(jStr.Split(" "))
                                If factorArr.Contains(jArr.Item(0)) AndAlso jArr.Count() > 0 AndAlso jArr.Item(1).Equals("RAW") Then
                                    sw = writerHash.Item("INSTRUMENT_FACTOR")
                                    lineWrite = prevBusDate.ToString("d") + "|"
                                    lineWrite += IdentifiersGet(lineArr, hdrNmIdxHash, "|") + "|"
                                    lineWrite += jArr.Item(0) + "|"
                                    lineWrite += lineArr.Item(hdrNmIdxHash.Item(jStr))
                                    sw.WriteLine(lineWrite)
                                ElseIf factorArr.Contains(jArr.Item(0)) AndAlso jArr.Count() > 0 AndAlso _
                                (jArr.Item(1).Equals("GLOBALRANK") Or _
                                 jArr.Item(1).Equals("UNIVERSERANK") Or _
                                 jArr.Item(1).Equals("SECTORRANK") Or _
                                 jArr.Item(1).Equals("SEGMENTRANK")) Then
                                    sw = writerHash.Item(jStr)
                                    lineWrite = prevBusDate.ToString("d") + "|" + prevBusDate.ToString("d") + "|"
                                    lineWrite += IdentifiersGet(lineArr, hdrNmIdxHash, "|") + "|"
                                    If hdrNmIdxHash.ContainsKey(jArr.Item(0) + " RAW") Then
                                        lineWrite += lineArr.Item(hdrNmIdxHash.Item(jArr.Item(0) + " RAW")) + "|"
                                    Else
                                        lineWrite += "|"
                                    End If
                                    lineWrite += lineArr.Item(hdrNmIdxHash.Item(jStr))
                                    sw.WriteLine(lineWrite)
                                ElseIf categoryArr.Contains(jArr.Item(0)) AndAlso jArr.Count() > 0 Then
                                    sw = writerHash.Item("CATEGORY_SCORE")
                                    lineWrite = prevBusDate.ToString("d") + "|" + strategy + "|"
                                    lineWrite += IdentifiersGet(lineArr, hdrNmIdxHash, "|") + "|"
                                    lineWrite += jArr.Item(1) + "|"
                                    lineWrite += jArr.Item(0) + "|"
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
            sw.WriteLine("$DATE" + ControlChars.Tab + "$Benchmark_cd")
            sw.WriteLine(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + universeCd + ControlChars.Quote)
            sw.Close()

            ConsoleWriteLine("<<<------ ModelRanksDailyLoad: ProcessFile End <<<------")
            ConsoleWriteLine("------>>> ModelRanksDailyLoad: StagingTablesLoad Begin ------>>>")

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

            bcpBatCount = 1
            batFile = dirTempStr + "bcp" + CStr(bcpBatCount) + ".BAT"
            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            sw.WriteLine("bcp QER..cusip2mqa_id_staging in " + dirTempStr + "CusipMqaId.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            sqlQuery = "DELETE cusip2mqa_id_staging"
            file = New FileInfo(dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP")
            If file.Exists() Then
                sw.WriteLine("bcp QER..universe_factset_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sqlQuery += ControlChars.NewLine + "DELETE universe_factset_staging"
            End If
            file = New FileInfo(dirTempStr + "CATEGORY_SCORE.BCP")
            If file.Exists() Then
                sw.WriteLine("bcp QER..category_score_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sqlQuery += ControlChars.NewLine + "DELETE category_score_staging"
            End If
            file = New FileInfo(dirTempStr + "SCORES.BCP")
            If file.Exists() Then
                sw.WriteLine("bcp QER..scores_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sqlQuery += ControlChars.NewLine + "DELETE scores_staging"
            End If
            file = New FileInfo(dirTempStr + "INSTRUMENT_FACTOR.BCP")
            If file.Exists() Then
                sw.WriteLine("bcp QER..instrument_factor_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sqlQuery += ControlChars.NewLine + "DELETE instrument_factor_staging"
            End If
            sw.Close()

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

            sqlQuery = "SELECT DISTINCT mqa_id FROM cusip2mqa_id_staging" + ControlChars.NewLine
            sqlQuery += " WHERE mqa_id IS NOT NULL" + ControlChars.NewLine
            sqlQuery += " ORDER BY mqa_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            ConsoleWriteLine(dirTempStr + "MQA_ID.QAP")
            sw = New StreamWriter(dirTempStr + "MQA_ID.QAP")
            sw.WriteLine("$ID")
            While rsReader.Read()
                sw.WriteLine(ControlChars.Quote + rsReader.GetString(0) + ControlChars.Quote)
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

            batFile = dirTempStr + "UniverseBySecurity.BAT"
            outputFile = ControlChars.Quote + dirTempStr + "UniverseBySecurity.CSV" + ControlChars.Quote + " "
            paramFile1 = ControlChars.Quote + dirTempStr + "MQA_ID.QAP" + ControlChars.Quote + " "
            paramFile2 = ControlChars.Quote + dirTempStr + "DATE.QAP" + ControlChars.Quote + " "

            ConsoleWriteLine(batFile)
            sw = New StreamWriter(batFile)
            sw.WriteLine(mqaExe + mqaUniverseQuery + outputFile + paramFile1 + paramFile2 + "/fc")
            sw.Close()

            Do
                Shell(batFile, , True)
                file = New FileInfo(dirTempStr + "UniverseBySecurity.CSV")
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

            iArr = New ArrayList(dir.GetFiles("UniverseBySecurity*.CSV"))
            GenerateBcpFiles(iArr, removeStringsMqa, 1)

            sr = New StreamReader(dirTempStr + "UniverseBySecurity.BCP")
            sw = New StreamWriter(dirTempStr + "UniverseBySecurity2.BCP")
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

            file = New FileInfo(dirTempStr + "CHAR_MQA_ID.QAP")
            If file.Exists() Then
                sw = New StreamWriter(dirTempStr + "CHAR_DATE.QAP")
                sw.WriteLine("$StartDate" + ControlChars.Tab + "$EndDate")
                sw.WriteLine(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + prevBusDate + ControlChars.Quote)
                sw.Close()

                batFile = dirTempStr + "Characteristics.BAT"
                outputFile = ControlChars.Quote + dirTempStr + "CHARACTERISTICS.CSV" + ControlChars.Quote + " "
                paramFile1 = ControlChars.Quote + dirTempStr + "CHAR_MQA_ID.QAP" + ControlChars.Quote + " "
                paramFile2 = ControlChars.Quote + dirTempStr + "CHAR_DATE.QAP" + ControlChars.Quote + " "

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

                iArr = New ArrayList(dir.GetFiles("CHARACTERISTICS*.CSV"))
                GenerateBcpFiles(iArr, removeStringsMqa, 1)
            End If

            bcpBatCount += 1
            batFile = dirTempStr + "bcp" + CStr(bcpBatCount) + ".BAT"
            sw = New StreamWriter(batFile)
            sw.WriteLine("bcp QER..universe_makeup_staging in " + dirTempStr + "UniverseBySecurity2.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            file = New FileInfo(dirTempStr + "CHARACTERISTICS.BCP")
            If file.Exists() Then
                sw.WriteLine("bcp QER..instrument_characteristics_staging in " + dirTempStr + "CHARACTERISTICS.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            End If
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

            sqlQuery = "INSERT universe_makeup_staging (universe_dt, universe_cd, ticker, cusip, sedol, isin, gv_key)" + ControlChars.NewLine
            sqlQuery += "SELECT universe_dt, '" + universeCd + "', ticker, cusip, sedol, isin, gv_key" + ControlChars.NewLine
            sqlQuery += "  FROM universe_factset_staging" + ControlChars.NewLine
            sqlQuery += " WHERE mqa_id IS NULL" + ControlChars.NewLine
            sqlQuery += "    OR cusip NOT IN (SELECT cusip FROM universe_makeup_staging WHERE cusip IS NOT NULL)"
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

            jArr = New ArrayList()
            file = New FileInfo(dirTempStr + "INSTRUMENT_FACTOR.BCP")
            If file.Exists() Then
                jArr.Add("instrument_factor_staging")
            End If
            file = New FileInfo(dirTempStr + "SCORES.BCP")
            If file.Exists() Then
                jArr.Add("scores_staging")
            End If
            file = New FileInfo(dirTempStr + "CATEGORY_SCORE.BCP")
            If file.Exists() Then
                jArr.Add("category_score_staging")
            End If

            For Each jStr In jArr
                For Each iStr In iArr
                    sqlQuery = "UPDATE " + jStr + ControlChars.NewLine
                    sqlQuery += "   SET " + iStr + " = p." + iStr + ControlChars.NewLine
                    sqlQuery += "  FROM cusip2mqa_id_staging c, universe_makeup_staging p" + ControlChars.NewLine
                    sqlQuery += " WHERE p.universe_dt = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND p.cusip = c.mqa_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + ".bdate = c.bdate" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + ".cusip = c.input_cusip" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + "." + iStr + " IS NULL"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")
                Next
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

            ConsoleWriteLine("<<<------ ModelRanksDailyLoad: StagingTablesLoad End <<<------")
            ConsoleWriteLine("------>>> ModelRanksDailyLoad: PermanentTablesLoad Begin ------>>>")

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

            file = New FileInfo(dirTempStr + "INSTRUMENT_FACTOR.BCP")
            If file.Exists() Then
                sqlQuery = "EXEC instrument_factor_load @SOURCE_CD='FS'"
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()
            End If

            file = New FileInfo(dirTempStr + "CATEGORY_SCORE.BCP")
            If file.Exists() Then
                sqlQuery = "EXEC category_score_load"
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()
            End If

            file = New FileInfo(dirTempStr + "SCORES.BCP")
            If file.Exists() Then
                sqlQuery = "EXEC scores_load"
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()

                sqlQuery = "DECLARE @STRATEGY_ID int" + ControlChars.NewLine
                sqlQuery += "SELECT @STRATEGY_ID = strategy_id FROM strategy" + ControlChars.NewLine
                sqlQuery += " WHERE strategy_cd = '" + strategy + "'" + ControlChars.NewLine
                sqlQuery += "EXEC model_portfolio_populate @BDATE='" + prevBusDate.ToString("d") + "', @STRATEGY_ID=@STRATEGY_ID"
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()
            End If

            sqlQuery = "SELECT f.factor_cd" + ControlChars.NewLine
            sqlQuery += "  FROM strategy g, factor_against_weight w, factor f" + ControlChars.NewLine
            sqlQuery += " WHERE g.strategy_cd = '" + strategy + "'" + ControlChars.NewLine
            sqlQuery += "   AND g.factor_model_id = w.factor_model_id" + ControlChars.NewLine
            sqlQuery += "   AND w.against = 'U'" + ControlChars.NewLine
            sqlQuery += "   AND w.factor_id = f.factor_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            jArr = New ArrayList()
            While rsReader.Read()
                jArr.Add(rsReader.GetString(0))
            End While
            rsReader.Close()

            iArr = New ArrayList(dir.GetFiles("*_UNIVERSERANK.BCP"))
            iArr.AddRange(New ArrayList(dir.GetFiles("*_GLOBALRANK.BCP")))
            For Each file In iArr
                i = file.Name().LastIndexOf("_")
                iStr = file.Name().Substring(0, i)

                If jArr.Contains(iStr) Then
                    bcpBatCount += 1
                    batFile = dirTempStr + "bcp" + CStr(bcpBatCount) + ".BAT"
                    sw = New StreamWriter(batFile)
                    sw.WriteLine("bcp QER..rank_staging in " + file.FullName() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                    sw.Close()

                    sqlQuery = "DELETE rank_staging"
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
                            ConsoleWriteLine("Terminating program...")
                            Exit Try
                        End If

                        bcpAttemptCount += 1
                    Loop While Not rowCountOkay

                    sqlQuery = "EXEC rank_load @UNIVERSE_CD='" + universeCd + "', @FACTOR_CD='" + iStr + "', @GROUPS=" + CStr(fractile) + ", @AGAINST='U'"
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                End If
            Next

            iArr = New ArrayList(dir.GetFiles("*SECTORRANK.BCP"))
            For Each file In iArr
                i = file.Name().LastIndexOf("_")
                iStr = file.Name().Substring(0, i)

                bcpBatCount += 1
                batFile = dirTempStr + "bcp" + CStr(bcpBatCount) + ".BAT"
                sw = New StreamWriter(batFile)
                sw.WriteLine("bcp QER..rank_staging in " + file.FullName() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sw.Close()

                sqlQuery = "DELETE rank_staging"
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
                        ConsoleWriteLine("Terminating program...")
                        Exit Try
                    End If

                    bcpAttemptCount += 1
                Loop While Not rowCountOkay

                sqlQuery = "SELECT w.against_id" + ControlChars.NewLine
                sqlQuery += "  FROM strategy g, factor_against_weight w, factor f" + ControlChars.NewLine
                sqlQuery += " WHERE g.strategy_cd = '" + strategy + "'" + ControlChars.NewLine
                sqlQuery += "   AND g.factor_model_id = w.factor_model_id" + ControlChars.NewLine
                sqlQuery += "   AND f.factor_cd = '" + iStr + "'" + ControlChars.NewLine
                sqlQuery += "   AND f.factor_id = w.factor_id" + ControlChars.NewLine
                sqlQuery += "   AND w.against = 'C'"
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                jArr = New ArrayList()
                While rsReader.Read()
                    jArr.Add(rsReader.GetInt32(0))
                End While
                rsReader.Close()

                For Each j In jArr
                    sqlQuery = "EXEC rank_load @UNIVERSE_CD='" + universeCd + "', @FACTOR_CD='" + iStr + "', @GROUPS=" + CStr(fractile) + ", @AGAINST='C', @AGAINST_ID=" + CStr(j)
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                Next
            Next

            iArr = New ArrayList(dir.GetFiles("*SEGMENTRANK.BCP"))
            For Each file In iArr
                i = file.Name().LastIndexOf("_")
                iStr = file.Name().Substring(0, i)

                bcpBatCount += 1
                batFile = dirTempStr + "bcp" + CStr(bcpBatCount) + ".BAT"
                sw = New StreamWriter(batFile)
                sw.WriteLine("bcp QER..rank_staging in " + file.FullName() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
                sw.Close()

                sqlQuery = "DELETE rank_staging"
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
                        ConsoleWriteLine("Terminating program...")
                        Exit Try
                    End If

                    bcpAttemptCount += 1
                Loop While Not rowCountOkay

                sqlQuery = "SELECT w.against_id" + ControlChars.NewLine
                sqlQuery += "  FROM strategy g, factor_against_weight w, factor f" + ControlChars.NewLine
                sqlQuery += " WHERE g.strategy_cd = '" + strategy + "'" + ControlChars.NewLine
                sqlQuery += "   AND g.factor_model_id = w.factor_model_id" + ControlChars.NewLine
                sqlQuery += "   AND f.factor_cd = '" + iStr + "'" + ControlChars.NewLine
                sqlQuery += "   AND f.factor_id = w.factor_id" + ControlChars.NewLine
                sqlQuery += "   AND w.against = 'G'"
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                jArr = New ArrayList()
                While rsReader.Read()
                    jArr.Add(rsReader.GetInt32(0))
                End While
                rsReader.Close()

                For Each j In jArr
                    sqlQuery = "EXEC rank_load @UNIVERSE_CD='" + universeCd + "', @FACTOR_CD='" + iStr + "', @GROUPS=" + CStr(fractile) + ", @AGAINST='G', @AGAINST_ID=" + CStr(j)
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                Next
            Next

            sqlQuery = "DECLARE @UNIVERSE_ID int" + ControlChars.NewLine
            sqlQuery += "SELECT @UNIVERSE_ID = universe_id FROM strategy WHERE strategy_cd = '" + strategy + "'" + ControlChars.NewLine + ControlChars.NewLine
            sqlQuery += "UPDATE rank_output" + ControlChars.NewLine
            sqlQuery += "   SET mqa_id = i.mqa_id," + ControlChars.NewLine
            sqlQuery += "       ticker = i.ticker," + ControlChars.NewLine
            sqlQuery += "       sedol = i.sedol," + ControlChars.NewLine
            sqlQuery += "       isin = i.isin," + ControlChars.NewLine
            sqlQuery += "       gv_key = i.gv_key" + ControlChars.NewLine
            sqlQuery += "  FROM cusip2mqa_id_staging c, instrument_characteristics i, rank_inputs r" + ControlChars.NewLine
            sqlQuery += " WHERE c.bdate = '" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
            sqlQuery += "   AND c.bdate = i.bdate" + ControlChars.NewLine
            sqlQuery += "   AND c.mqa_cusip = i.cusip" + ControlChars.NewLine
            sqlQuery += "   AND r.bdate = i.bdate" + ControlChars.NewLine
            sqlQuery += "   AND r.universe_id = @UNIVERSE_ID" + ControlChars.NewLine
            sqlQuery += "   AND r.rank_event_id = rank_output.rank_event_id" + ControlChars.NewLine
            sqlQuery += "   AND rank_output.cusip = c.input_cusip"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")

            sqlQuery = "DECLARE @UNIVERSE_ID int, @SECTOR_MODEL_ID int" + ControlChars.NewLine + ControlChars.NewLine
            sqlQuery += "SELECT @UNIVERSE_ID = g.universe_id," + ControlChars.NewLine
            sqlQuery += "       @SECTOR_MODEL_ID = f.sector_model_id" + ControlChars.NewLine
            sqlQuery += "  FROM strategy g, factor_model f" + ControlChars.NewLine
            sqlQuery += " WHERE g.strategy_cd = '" + strategy + "'" + ControlChars.NewLine
            sqlQuery += "   AND g.factor_model_id = f.factor_model_id" + ControlChars.NewLine + ControlChars.NewLine
            sqlQuery += "EXEC sector_model_security_populate @BDATE='" + prevBusDate.ToString("d") + "', @UNIVERSE_DT='" + prevBusDate.ToString("d") + "', @UNIVERSE_ID=@UNIVERSE_ID, @SECTOR_MODEL_ID=@SECTOR_MODEL_ID"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()
            dbConn.Close()

            ConsoleWriteLine("<<<------ ModelRanksDailyLoad: PermanentTablesLoad End <<<------")
            ConsoleWriteLine("------>>> ModelRanksDailyLoad: Archive Begin ------>>>")
            If Not dirArchive.Exists() Then
                dirArchive.Create()
            End If
            ConsoleWriteLine("Archive Directory = " + dirArchiveStr)
            ArchiveTempDir(dirTempStr, dirArchiveStr)
            ConsoleWriteLine("<<<------ ModelRanksDailyLoad: Archive End <<<------")
            ConsoleWriteLine("<<<------ ModelRanksDailyLoad: Main End <<<------")
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
