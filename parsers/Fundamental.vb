Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading
Imports Utility

Module Fundamental

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

        Dim host As String = System.Configuration.ConfigurationSettings.AppSettings("FtpHost")
        Dim user As String = System.Configuration.ConfigurationSettings.AppSettings("FtpUser")
        Dim pw As String = System.Configuration.ConfigurationSettings.AppSettings("FtpPw")

        Dim rowCountOkay As Boolean
        Dim i As Integer
        Dim dirPickUpStr, dirArchiveStr, dirTempStr, fileName, mqaEXE, _
            mqaCusipQuery, mqaUniverseQuery, mqaCharQuery, mqaBatFile, bcpBatFile, _
            outputFile, paramFile1, paramFile2, strategy, lineWrite, strTemp, iStr, jStr As String
        Dim lineArr As String() = Nothing
        Dim removeStrings As String() = {"NULL", "ERR"}
        Dim prevBusDate As DateTime
        Dim iArr, jArr, filesArr, strategyIdArr, allFactorsArr As ArrayList
        Dim sr As StreamReader
        Dim sw As StreamWriter
        Dim file As FileInfo
        Dim dirPickUp, dirTemp, dirArchive As DirectoryInfo
        Dim columnFactor, columnFileWriter, identifiers As Hashtable

        Try
            ConsoleWriteLine("------>>> Fundamental: Main Begin ------>>>")

            If args.Length() < 1 Then
                Console.Write("Select strategy: ")
                strategy = Console.ReadLine().ToUpper()
            Else
                strategy = CStr(args.GetValue(0)).ToUpper()
            End If

            ConsoleWriteLine("Database Server = " + dbServerStr)
            ConsoleWriteLine("Database = " + dbStr)
            ConsoleWriteLine("Strategy = " + strategy)

            If Not strategy.Equals("MCG") And Not strategy.Equals("LCG") Then
                ConsoleWriteLine("ERROR: Invalid strategy parameter passed!")
                ConsoleWriteLine("Terminating program...")
                Exit Try
            End If

            ConsoleWriteLine("------>>> Fundamental: Initialization Begin ------>>>")

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn

            sqlQuery = "SELECT code, decode FROM decode WHERE item = 'DIR' AND code IN ('ARCHIVE','CORNERSTONE','MQA_QUERIES_SPECIAL')" + ControlChars.NewLine
            sqlQuery += "UNION" + ControlChars.NewLine
            sqlQuery += "SELECT code, decode FROM decode WHERE item = 'FILE' AND code IN ('" + strategy + "_DATA','QALPROC_EXE')"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                If rsReader.GetString(0).Equals("ARCHIVE") Then
                    dirArchiveStr = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("CORNERSTONE") Then
                    dirPickUpStr = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("MCG_DATA") Or rsReader.GetString(0).Equals("LCG_DATA") Then
                    fileName = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("QALPROC_EXE") Then
                    mqaEXE = ControlChars.Quote + rsReader.GetString(1) + ControlChars.Quote + " "
                ElseIf rsReader.GetString(0).Equals("MQA_QUERIES_SPECIAL") Then
                    mqaCusipQuery = ControlChars.Quote + rsReader.GetString(1) + "CUSIP2MQA_ID.QAL" + ControlChars.Quote + " "
                    mqaUniverseQuery = ControlChars.Quote + rsReader.GetString(1) + "UNIVERSE_BY_SECURITY.QAL" + ControlChars.Quote + " "
                    mqaCharQuery = ControlChars.Quote + rsReader.GetString(1) + "CHARACTERISTICS.QAL" + ControlChars.Quote + " "
                End If
            End While
            rsReader.Close()

            dirArchive = New DirectoryInfo(dirArchiveStr)
            dirPickUp = New DirectoryInfo(dirPickUpStr)
            If Not dirPickUp.Exists() Then
                ConsoleWriteLine("ERROR: Directory " + dirPickUp.FullName() + " does not exist!")
                ConsoleWriteLine("Terminating program...")
                Exit Try
            End If

            sqlQuery = "SELECT convert(varchar, getdate(), 112) + '_' + convert(varchar, getdate(), 108)"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                dirTempStr = "C:\temp_Fundamental_" + strategy + "_" + rsReader.GetString(0).Replace(":", "") + "\"
            End While
            rsReader.Close()

            dirTemp = New DirectoryInfo(dirTempStr)
            If Not dirTemp.Exists() Then
                dirTemp.Create()
            End If

            sqlQuery = "SELECT factor_cd FROM factor"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            allFactorsArr = New ArrayList
            While rsReader.Read()
                allFactorsArr.Add(rsReader.GetString(0))
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
            dbConn.Close()

            ConsoleWriteLine("<<<------ Fundamental: Initialization End <<<------")
            ConsoleWriteLine("------>>> Fundamental: FindFile Begin ------>>>")

            While True
                file = New FileInfo(dirPickUpStr + fileName)
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

            ConsoleWriteLine("<<<------ Fundamental: FindFile End <<<------")
            ConsoleWriteLine("------>>> Fundamental: ProcessFile Begin ------>>>")

            file = New FileInfo(dirPickUpStr + fileName)
            If file.Exists() Then
                columnFactor = New Hashtable
                columnFileWriter = New Hashtable
                identifiers = New Hashtable

                sr = file.OpenText()
                For i = 1 To 2
                    If sr.Peek() >= 0 Then
                        sr.ReadLine()
                    End If
                Next

                lineArr = ChangeDelimiter(sr.ReadLine(), ",", "|").Split("|")
                For i = 0 To lineArr.Length() - 1
                    strTemp = Trim(lineArr.GetValue(i))
                    If strTemp.StartsWith(ControlChars.Quote) And strTemp.EndsWith(ControlChars.Quote) Then
                        strTemp = strTemp.Substring(1, strTemp.Length() - 2)
                    End If
                    strTemp = Trim(UCase(strTemp))

                    If strTemp.Equals("TICKER") Or strTemp.Equals("CUSIP") Or strTemp.Equals("SEDOL") Or strTemp.Equals("ISIN") Or strTemp.Equals("GVKEY") _
                    Or strTemp.Equals("GICS_SUBIND_NUM") Or strTemp.Equals("RUSSELL_SECTOR_NUM") Or strTemp.Equals("RUSSELL_IND_NUM") Then
                        If Not identifiers.ContainsKey(strTemp) Then
                            identifiers.Add(strTemp, i)
                        End If
                    ElseIf allFactorsArr.Contains(strTemp) Then
                        columnFactor.Add(i, strTemp)
                        ConsoleWriteLine(dirTempStr + "FactorColumn" + i.ToString() + "_" + strTemp + ".BCP")
                        sw = New StreamWriter(dirTempStr + "FactorColumn" + i.ToString() + "_" + strTemp + ".BCP")
                        sw.AutoFlush = True
                        columnFileWriter.Add(i, sw)
                    End If
                Next

                ConsoleWriteLine(dirTempStr + "CUSIP.QAP")
                sw = New StreamWriter(dirTempStr + "CUSIP.QAP")
                sw.AutoFlush = True
                sw.WriteLine("$CUSIP")
                columnFileWriter.Add("CUSIP", sw)

                ConsoleWriteLine(dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP")
                sw = New StreamWriter(dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP")
                sw.AutoFlush = True
                columnFileWriter.Add("UNIVERSE_FACTSET_STAGING", sw)

                ConsoleWriteLine(dirTempStr + "DATE.QAP")
                sw = New StreamWriter(dirTempStr + "DATE.QAP")
                sw.AutoFlush = True
                sw.WriteLine("$DATE" + ControlChars.Tab + "$Benchmark_cd")
                sw.Write(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote)
                If strategy.Equals("MCG") Then
                    sw.WriteLine("MCG" + ControlChars.Quote)
                ElseIf strategy.Equals("LCG") Then
                    sw.WriteLine("LCG" + ControlChars.Quote)
                End If
                sw.Close()

                sr.ReadLine()
                ConsoleWrite("Processing")

                While sr.Peek() >= 0
                    Console.Write(".")
                    lineArr = ChangeDelimiter(sr.ReadLine(), ",", "|").Split("|")
                    For i = 0 To lineArr.Length() - 1
                        strTemp = Trim(lineArr.GetValue(i))
                        If strTemp.StartsWith(ControlChars.Quote) And strTemp.EndsWith(ControlChars.Quote) Then
                            strTemp = strTemp.Substring(1, strTemp.Length() - 2)
                        End If
                        strTemp = Trim(UCase(strTemp))
                        If strTemp.Equals("NA") Or strTemp.Equals("@NA") Then
                            strTemp = ""
                        End If
                        lineArr.SetValue(strTemp, i)
                    Next

                    lineWrite = ControlChars.Quote + lineArr.GetValue(identifiers.Item("CUSIP")) + ControlChars.Quote
                    sw = columnFileWriter.Item("CUSIP")
                    sw.WriteLine(lineWrite)

                    lineWrite = prevBusDate.ToString("d") + "||"
                    lineWrite += lineArr.GetValue(identifiers.Item("TICKER")) + "|"
                    lineWrite += lineArr.GetValue(identifiers.Item("CUSIP")) + "|"
                    lineWrite += lineArr.GetValue(identifiers.Item("SEDOL")) + "|"
                    lineWrite += lineArr.GetValue(identifiers.Item("ISIN")) + "|"
                    strTemp = lineArr.GetValue(identifiers.Item("GVKEY"))
                    If IsNumeric(strTemp) Then
                        lineWrite += CStr(CInt(strTemp)) + "|"
                    Else
                        lineWrite += "|"
                    End If
                    strTemp = lineArr.GetValue(identifiers.Item("GICS_SUBIND_NUM"))
                    If IsNumeric(strTemp) Then
                        lineWrite += CStr(CInt(strTemp)) + "|"
                    Else
                        lineWrite += "|"
                    End If
                    strTemp = lineArr.GetValue(identifiers.Item("RUSSELL_SECTOR_NUM"))
                    If IsNumeric(strTemp) Then
                        lineWrite += CStr(CInt(strTemp)) + "|"
                    Else
                        lineWrite += "|"
                    End If
                    strTemp = lineArr.GetValue(identifiers.Item("RUSSELL_IND_NUM"))
                    If IsNumeric(strTemp) Then
                        lineWrite += CStr(CInt(strTemp))
                    End If
                    sw = columnFileWriter.Item("UNIVERSE_FACTSET_STAGING")
                    sw.WriteLine(lineWrite)

                    For i = 0 To lineArr.Length() - 1
                        If columnFactor.ContainsKey(i) Then
                            lineWrite = prevBusDate.ToString("d") + "||"
                            lineWrite += lineArr.GetValue(identifiers.Item("TICKER")) + "|"
                            lineWrite += lineArr.GetValue(identifiers.Item("CUSIP")) + "|"
                            lineWrite += lineArr.GetValue(identifiers.Item("SEDOL")) + "|"
                            lineWrite += lineArr.GetValue(identifiers.Item("ISIN")) + "|"
                            strTemp = lineArr.GetValue(identifiers.Item("GVKEY"))
                            If IsNumeric(strTemp) Then
                                lineWrite += CStr(CInt(strTemp)) + "|"
                            Else
                                lineWrite += "|"
                            End If
                            lineWrite += columnFactor.Item(i) + "|"
                            lineWrite += lineArr.GetValue(i)
                            sw = columnFileWriter.Item(i)
                            sw.WriteLine(lineWrite)
                        End If
                    Next
                End While

                Console.WriteLine("completed")
                sr.Close()

                For Each sw In columnFileWriter.Values()
                    sw.Close()
                Next
            Else
                ConsoleWriteLine("ERROR: " + file.FullName() + " DOES NOT EXIST!")
                ConsoleWriteLine("Terminating program...")
                Exit Try
            End If

            ConsoleWriteLine("<<<------ Fundamental: ProcessFile End <<<------")
            ConsoleWriteLine("------>>> Fundamental: StagingTablesLoad Begin ------>>>")
            'run cusip2mqa script
            'bcp cusip2mqa_staging
            'bcp universe_factset_staging
            'bcp instrument_factor_staging
            'run universe_by_security script
            'run characteristics
            'bcp universe_makeup_staging
            'bcp instrument_characteristics_staging
            mqaBatFile = dirTempStr + "Cusip2MqaId.BAT"
            bcpBatFile = dirTempStr + "bcp01.BAT"

            outputFile = ControlChars.Quote + dirTempStr + "CusipMqaId.CSV" + ControlChars.Quote + " "
            paramFile1 = ControlChars.Quote + dirTempStr + "CUSIP.QAP" + ControlChars.Quote + " "
            paramFile2 = ControlChars.Quote + dirTempStr + "DATE.QAP" + ControlChars.Quote + " "

            ConsoleWriteLine(mqaBatFile)
            sw = New StreamWriter(mqaBatFile)
            sw.WriteLine(mqaEXE + mqaCusipQuery + outputFile + paramFile1 + paramFile2 + "/fq")
            sw.Close()

            Do
                Shell(mqaBatFile, , True)
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

            filesArr = New ArrayList(dirTemp.GetFiles("FactorColumn*.BCP"))
            ConsoleWriteLine(bcpBatFile)
            sw = New StreamWriter(bcpBatFile)
            sw.WriteLine("bcp QER..cusip2mqa_id_staging in " + dirTempStr + "CusipMqaId.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            sw.WriteLine("bcp QER..universe_factset_staging in " + dirTempStr + "UNIVERSE_FACTSET_STAGING.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            For Each file In filesArr
                sw.WriteLine("bcp QER..instrument_factor_staging in " + dirTempStr + file.Name() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
            Next
            sw.Close()

            sqlQuery = "DELETE cusip2mqa_id_staging" + ControlChars.NewLine
            sqlQuery += "DELETE universe_factset_staging" + ControlChars.NewLine
            sqlQuery += "DELETE instrument_factor_staging"

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

                Shell(bcpBatFile, , True)
                rowCountOkay = BcpConfirmRowCount(bcpBatFile)
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

            mqaBatFile = dirTempStr + "UniverseBySecurity.BAT"

            outputFile = ControlChars.Quote + dirTempStr + "UniverseBySecurity.CSV" + ControlChars.Quote + " "
            paramFile1 = ControlChars.Quote + dirTempStr + "MQA_ID.QAP" + ControlChars.Quote + " "
            paramFile2 = ControlChars.Quote + dirTempStr + "DATE.QAP" + ControlChars.Quote + " "

            sw = New StreamWriter(mqaBatFile)
            sw.WriteLine(mqaEXE + mqaUniverseQuery + outputFile + paramFile1 + paramFile2 + "/fc")
            sw.Close()

            Do
                Shell(mqaBatFile, , True)
                file = New FileInfo(dirTempStr + "UniverseBySecurity.CSV")
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

            filesArr = New ArrayList(dirTemp.GetFiles("UniverseBySecurity*.CSV"))
            GenerateBcpFiles(filesArr, removeStrings, 1)

            sr = New StreamReader(dirTempStr + "UniverseBySecurity.BCP")
            sw = New StreamWriter(dirTempStr + "UniverseBySecurity2.BCP")
            While sr.Peek() >= 0
                lineArr = sr.ReadLine().Split("|")
                sw.Write(lineArr.GetValue(3))
                For i = 0 To lineArr.Length() - 1
                    sw.Write("|" + lineArr.GetValue(i))
                Next
                sw.WriteLine()
            End While
            sw.Close()
            sr.Close()

            file = New FileInfo(dirTempStr + "CHAR_MQA_ID.QAP")
            If file.Exists() Then
                mqaBatFile = dirTempStr + "CHARACTERISTICS.BAT"
                bcpBatFile = dirTempStr + "bcp02.BAT"

                sw = New StreamWriter(dirTempStr + "CHAR_DATE.QAP")
                sw.WriteLine("$StartDate" + ControlChars.Tab + "$EndDate")
                sw.WriteLine(ControlChars.Quote + prevBusDate.ToString("d") + ControlChars.Quote + ControlChars.Tab + ControlChars.Quote + prevBusDate + ControlChars.Quote)
                sw.Close()

                outputFile = ControlChars.Quote + dirTempStr + "CHARACTERISTICS.CSV" + ControlChars.Quote + " "
                paramFile1 = ControlChars.Quote + dirTempStr + "CHAR_MQA_ID.QAP" + ControlChars.Quote + " "
                paramFile2 = ControlChars.Quote + dirTempStr + "CHAR_DATE.QAP" + ControlChars.Quote + " "

                sw = New StreamWriter(mqaBatFile)
                sw.WriteLine(mqaEXE + mqaCharQuery + outputFile + paramFile1 + paramFile2 + "/fc")
                sw.Close()

                Do
                    Shell(mqaBatFile, , True)
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
            End If

            sw = New StreamWriter(bcpBatFile)
            sw.WriteLine("bcp QER..universe_makeup_staging in " + dirTempStr + "UniverseBySecurity2.BCP -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T")
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

                Shell(bcpBatFile, , True)
                rowCountOkay = BcpConfirmRowCount(bcpBatFile)
                ConsoleWriteLine("BcpConfirmRowCount = " + UCase(rowCountOkay.ToString()))

                If bcpAttemptCount >= bcpAttemptsMax Then
                    ConsoleWriteLine("ERROR: BCP failed after " + CStr(bcpAttemptsMax) + " attempts!")
                    ConsoleWriteLine("Terminating program...")
                    Exit Try
                End If

                bcpAttemptCount += 1
            Loop While Not rowCountOkay

            ConsoleWriteLine("<<<------ Fundamental: StagingTablesLoad End <<<------")
            ConsoleWriteLine("------>>> Fundamental: UpdateInsertExecute Begin ------>>>")
            'update mqa_id's from cusip2mqa_id_staging to:
            '  universe_makeup_staging
            '  universe_factset_staging
            '  instrument_factor_staging
            '  instrument_characteristics_staging
            'insert missing securities to:
            '  universe_makeup_staging
            '  instrument_characteristics_staging
            'check rowcounts
            'check gics and russell sector info differences between factset and mqa
            'update gics/russell sector info from factset if different
            'update sedol, isin, gv_key from factset if missing from mqa
            'execute:
            '  universe_makeup_load
            '  instrument_characteristics_load
            '  instrument_factor_load

            sqlQuery = "UPDATE universe_factset_staging" + ControlChars.NewLine
            sqlQuery += "   SET mqa_id = c.mqa_id" + ControlChars.NewLine
            sqlQuery += "  FROM cusip2mqa_id_staging c" + ControlChars.NewLine
            sqlQuery += " WHERE universe_factset_staging.universe_dt = c.bdate" + ControlChars.NewLine
            sqlQuery += "   AND universe_factset_staging.cusip = c.input_cusip"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")

            sqlQuery = "UPDATE instrument_factor_staging" + ControlChars.NewLine
            sqlQuery += "   SET mqa_id = c.mqa_id" + ControlChars.NewLine
            sqlQuery += "  FROM cusip2mqa_id_staging c" + ControlChars.NewLine
            sqlQuery += " WHERE instrument_factor_staging.bdate = c.bdate" + ControlChars.NewLine
            sqlQuery += "   AND instrument_factor_staging.cusip = c.input_cusip"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")

            sqlQuery = "INSERT universe_makeup_staging (universe_dt, universe_cd, ticker, cusip, sedol, isin, gv_key)" + ControlChars.NewLine
            sqlQuery += "SELECT universe_dt, '" + strategy + "', ticker, cusip, sedol, isin, gv_key" + ControlChars.NewLine
            sqlQuery += "  FROM universe_factset_staging s" + ControlChars.NewLine
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
            iArr.Add("instrument_factor_staging")

            For Each iStr In iArr
                If iStr.Equals("instrument_factor_staging") Then
                    sqlQuery = "SELECT COUNT(DISTINCT cusip) FROM " + iStr
                Else
                    sqlQuery = "SELECT COUNT(*) FROM " + iStr
                End If
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                While rsReader.Read()
                    ConsoleWriteLine(CStr(rsReader.GetInt32(0)))
                End While
                rsReader.Close()
            Next

            iArr = New ArrayList()
            iArr.Add("sedol")
            iArr.Add("isin")
            iArr.Add("gv_key")

            jArr = New ArrayList()
            jArr.Add("instrument_characteristics_staging")
            jArr.Add("universe_makeup_staging")

            For Each jStr In jArr
                For Each iStr In iArr
                    sqlQuery = "UPDATE " + jStr + ControlChars.NewLine
                    sqlQuery += "   SET " + iStr + " = u." + iStr + ControlChars.NewLine
                    sqlQuery += "  FROM universe_factset_staging u" + ControlChars.NewLine
                    If jStr.Equals("instrument_characteristics_staging") Then
                        sqlQuery += " WHERE " + jStr + ".bdate = u.universe_dt" + ControlChars.NewLine
                    ElseIf jStr.Equals("universe_makeup_staging") Then
                        sqlQuery += " WHERE " + jStr + ".universe_dt = u.universe_dt" + ControlChars.NewLine
                    End If
                    sqlQuery += "   AND " + jStr + ".cusip = u.cusip" + ControlChars.NewLine
                    sqlQuery += "   AND " + jStr + "." + iStr + " IS NULL" + ControlChars.NewLine
                    sqlQuery += "   AND u." + iStr + " IS NOT NULL"
                    ConsoleWriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    Console.WriteLine(CStr(dbCommand.ExecuteNonQuery()) + " row(s) affected")
                Next
            Next

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

            ConsoleWriteLine("<<<------ Fundamental: UpdateInsertExecute End <<<------")
            ConsoleWriteLine("------>>> Fundamental: UniverseGrowthNonGrowth Begin ------>>>")

            sqlQuery = "DECLARE @UNIVERSE_ID int" + ControlChars.NewLine
            sqlQuery += "SELECT @UNIVERSE_ID = universe_id" + ControlChars.NewLine
            sqlQuery += "  FROM universe_def" + ControlChars.NewLine
            sqlQuery += " WHERE universe_cd = '" + strategy + "'" + ControlChars.NewLine
            sqlQuery += "EXEC universe_gng_populate @UNIVERSE_DT='" + prevBusDate.ToString("d") + "', @UNIVERSE_ID=@UNIVERSE_ID"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            dbCommand.ExecuteNonQuery()

            ConsoleWriteLine("<<<------ Fundamental: UniverseGrowthNonGrowth End <<<------")
            ConsoleWriteLine("------>>> Fundamental: RunRanksScores Begin ------>>>")

            sqlQuery = "SELECT strategy_id FROM strategy" + ControlChars.NewLine
            sqlQuery += " WHERE strategy_cd LIKE '" + strategy + "%' ORDER BY strategy_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            strategyIdArr = New ArrayList
            While rsReader.Read()
                strategyIdArr.Add(rsReader.GetInt32(0))
            End While
            rsReader.Close()

            iArr = New ArrayList()
            iArr.Add("strategy_ranks_run")
            iArr.Add("strategy_scores_compute")
            iArr.Add("model_portfolio_populate")

            For Each i In strategyIdArr
                For Each iStr In iArr
                    sqlQuery = "EXEC " + iStr + " @BDATE='" + prevBusDate.ToString("d") + "', @STRATEGY_ID=" + CStr(i)
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                Next
            Next

            ConsoleWriteLine("<<<------ Fundamental: RunRanksScores End <<<------")
            ConsoleWriteLine("------>>> Fundamental: CornerstoneFiles Begin ------>>>")

            sqlQuery = "SELECT p.cusip, CONVERT(varchar, p.universe_dt, 112)," + ControlChars.NewLine
            sqlQuery += "       CASE i.price_close WHEN 0.0 THEN 0.0 ELSE 1000.0 / i.price_close END" + ControlChars.NewLine
            sqlQuery += "  FROM universe_def d, universe_makeup p, instrument_characteristics i" + ControlChars.NewLine
            sqlQuery += " WHERE d.universe_cd = '" + strategy + "G_MPF_EQL'" + ControlChars.NewLine
            sqlQuery += "   AND d.universe_id = p.universe_id" + ControlChars.NewLine
            sqlQuery += "   AND p.universe_dt = '" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
            sqlQuery += "   AND p.universe_dt = i.bdate" + ControlChars.NewLine
            sqlQuery += "   AND p.cusip = i.cusip" + ControlChars.NewLine
            sqlQuery += " ORDER BY p.cusip"
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            Console.WriteLine("Writing " + dirTempStr + strategy + "G_MDL_PRTF.CSV")
            sw = New StreamWriter(dirTempStr + strategy + "G_MDL_PRTF.CSV")
            While rsReader.Read()
                lineWrite = ""
                For i = 0 To rsReader.FieldCount() - 1
                    If i <= 1 Then
                        If rsReader.IsDBNull(i) Then
                            lineWrite += "|"
                        Else
                            lineWrite += rsReader.GetString(i) + "|"
                        End If
                    Else
                        If Not rsReader.IsDBNull(i) Then
                            lineWrite += CStr(rsReader.GetDouble(i))
                        End If
                    End If
                Next
                sw.WriteLine(lineWrite)
            End While
            sw.Close()
            rsReader.Close()

            sqlQuery = "SELECT p.cusip, CONVERT(varchar, p.universe_dt, 112)," + ControlChars.NewLine
            sqlQuery += "       CASE i.price_close WHEN 0.0 THEN 0.0 ELSE 1000.0 / i.price_close END" + ControlChars.NewLine
            sqlQuery += "  FROM universe_def d, universe_makeup p, instrument_characteristics i" + ControlChars.NewLine
            sqlQuery += " WHERE d.universe_cd = '" + strategy + "NG_MPF_EQL'" + ControlChars.NewLine
            sqlQuery += "   AND d.universe_id = p.universe_id" + ControlChars.NewLine
            sqlQuery += "   AND p.universe_dt = '" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
            sqlQuery += "   AND p.universe_dt = i.bdate" + ControlChars.NewLine
            sqlQuery += "   AND p.cusip = i.cusip" + ControlChars.NewLine
            sqlQuery += " ORDER BY p.cusip"
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            Console.WriteLine("Writing " + dirTempStr + strategy + "NG_MDL_PRTF.CSV")
            sw = New StreamWriter(dirTempStr + strategy + "NG_MDL_PRTF.CSV")
            While rsReader.Read()
                lineWrite = ""
                For i = 0 To rsReader.FieldCount() - 1
                    If i <= 1 Then
                        If rsReader.IsDBNull(i) Then
                            lineWrite += "|"
                        Else
                            lineWrite += rsReader.GetString(i) + "|"
                        End If
                    Else
                        If Not rsReader.IsDBNull(i) Then
                            lineWrite += CStr(rsReader.GetDouble(i))
                        End If
                    End If
                Next
                sw.WriteLine(lineWrite)
            End While
            sw.Close()
            rsReader.Close()

            sqlQuery = "SELECT s.cusip, CONVERT(varchar, s.bdate, 112)," + ControlChars.NewLine
            sqlQuery += "       s.total_score, s.universe_score, s.ss_score" + ControlChars.NewLine
            sqlQuery += "  FROM strategy g, scores s" + ControlChars.NewLine
            sqlQuery += " WHERE g.strategy_cd = '" + strategy + "-G'" + ControlChars.NewLine
            sqlQuery += "   AND g.strategy_id = s.strategy_id" + ControlChars.NewLine
            sqlQuery += "   AND s.bdate = '" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
            sqlQuery += " ORDER BY s.cusip"
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            Console.WriteLine("Writing " + dirTempStr + strategy + "G_RANKS.CSV")
            sw = New StreamWriter(dirTempStr + strategy + "G_RANKS.CSV")
            While rsReader.Read()
                lineWrite = ""
                For i = 0 To rsReader.FieldCount() - 1
                    If i <= 1 Then
                        If rsReader.IsDBNull(i) Then
                            lineWrite += "|"
                        Else
                            lineWrite += rsReader.GetString(i) + "|"
                        End If
                    Else
                        If rsReader.IsDBNull(i) Then
                            lineWrite += "|"
                        Else
                            lineWrite += CStr(CInt(Math.Round(CDbl(rsReader.GetDouble(i))))) + "|"
                        End If
                    End If
                Next
                lineWrite = lineWrite.Substring(0, lineWrite.Length() - 1)
                sw.WriteLine(lineWrite)
            End While
            sw.Close()
            rsReader.Close()

            sqlQuery = "SELECT s.cusip, CONVERT(varchar, s.bdate, 112)," + ControlChars.NewLine
            sqlQuery += "       s.total_score, s.universe_score, s.ss_score" + ControlChars.NewLine
            sqlQuery += "  FROM strategy g, scores s" + ControlChars.NewLine
            sqlQuery += " WHERE g.strategy_cd = '" + strategy + "-NG'" + ControlChars.NewLine
            sqlQuery += "   AND g.strategy_id = s.strategy_id" + ControlChars.NewLine
            sqlQuery += "   AND s.bdate = '" + prevBusDate.ToString("d") + "'" + ControlChars.NewLine
            sqlQuery += " ORDER BY s.cusip"
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            Console.WriteLine("Writing " + dirTempStr + strategy + "NG_RANKS.CSV")
            sw = New StreamWriter(dirTempStr + strategy + "NG_RANKS.CSV")
            While rsReader.Read()
                lineWrite = ""
                For i = 0 To rsReader.FieldCount() - 1
                    If i <= 1 Then
                        If rsReader.IsDBNull(i) Then
                            lineWrite += "|"
                        Else
                            lineWrite += rsReader.GetString(i) + "|"
                        End If
                    Else
                        If rsReader.IsDBNull(i) Then
                            lineWrite += "|"
                        Else
                            lineWrite += CStr(CInt(Math.Round(CDbl(rsReader.GetDouble(i))))) + "|"
                        End If
                    End If
                Next
                lineWrite = lineWrite.Substring(0, lineWrite.Length() - 1)
                sw.WriteLine(lineWrite)
            End While
            sw.Close()
            rsReader.Close()
            dbConn.Close()

            sw = New StreamWriter(dirTempStr + "ftpcmd.txt")
            sw.WriteLine(user)
            sw.WriteLine(pw)
            sw.WriteLine("lcd " + dirTempStr.Substring(0, dirTempStr.Length() - 1))
            strTemp = ""

            filesArr = New ArrayList(dirTemp.GetFiles("*_MDL_PRTF.CSV"))
            If filesArr.Count() <= 0 Then
                ConsoleWriteLine("ERROR: No files found with names like *_MDL_PRTF.CSV !")
                ConsoleWriteLine("Terminating program...")
                Exit Try
            End If
            For Each file In filesArr
                strTemp += "Uploading " + file.Name() + " to ftp://" + host + ControlChars.NewLine
                sw.WriteLine("put " + file.Name())
            Next

            filesArr = New ArrayList(dirTemp.GetFiles("*_RANKS.CSV"))
            If filesArr.Count() <= 0 Then
                ConsoleWriteLine("ERROR: No files found with names like *_RANKS.CSV !")
                ConsoleWriteLine("Terminating program...")
                Exit Try
            End If
            For Each file In filesArr
                strTemp += "Uploading " + file.Name() + " to ftp://" + host + ControlChars.NewLine
                sw.WriteLine("put " + file.Name())
            Next

            sw.WriteLine("quit")
            sw.Close()

            sw = New StreamWriter(dirTempStr + "ftpftp.bat")
            sw.WriteLine("ftp -s:" + dirTempStr + "ftpcmd.txt " + host)
            sw.Close()

            If dbServerStr.EndsWith("P") Then 'TRANSFER FILES ONLY WHEN RUNNING IN PRODUCTION
                ConsoleWriteLine()
                Console.WriteLine(strTemp)
                Shell(dirTempStr + "ftpftp.bat", , True)
            End If

            ConsoleWriteLine("<<<------ Fundamental: CornerstoneFiles End <<<------")
            ConsoleWriteLine("------>>> Fundamental: Archive Begin ------>>>")
            If Not dirArchive.Exists() Then
                dirArchive.Create()
            End If
            ConsoleWriteLine("Archive Directory = " + dirArchiveStr)
            ArchiveTempDir(dirTempStr, dirArchiveStr)
            ConsoleWriteLine("<<<------ Fundamental: Archive End <<<------")
            ConsoleWriteLine("<<<------ Fundamental: Main End <<<------")
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
