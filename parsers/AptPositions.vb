Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading
Imports System.Net.Mail
Imports Utility

Module AptPositions

    Sub Main()
        Dim dbServerStr As String = System.Configuration.ConfigurationSettings.AppSettings("DbServer")
        Dim dbStr As String = System.Configuration.ConfigurationSettings.AppSettings("Database")
        Dim dbConnStr As String = "Integrated Security=SSPI; Data Source=" + dbServerStr + "; Initial Catalog=" + dbStr
        Dim dbConn As SqlConnection = New SqlConnection(dbConnStr)

        Dim sqlQuery As String
        Dim dbCommand As SqlCommand
        Dim rsReader As SqlDataReader

        Dim rowCountLimit, weeksBack As Integer

        If IsNumeric(System.Configuration.ConfigurationSettings.AppSettings("RowCountLimit")) Then
            rowCountLimit = CInt(System.Configuration.ConfigurationSettings.AppSettings("RowCountLimit"))
        Else
            rowCountLimit = 22000
        End If

        If IsNumeric(System.Configuration.ConfigurationSettings.AppSettings("WeeksBack")) Then
            weeksBack = CInt(System.Configuration.ConfigurationSettings.AppSettings("WeeksBack"))
        Else
            weeksBack = -2
        End If

        If weeksBack > 0 Then
            weeksBack = -1 * weeksBack
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
            dbTimeoutMinutes = 10
        End If

        If IsNumeric(System.Configuration.ConfigurationSettings.AppSettings("SleepIntervalMinutes")) Then
            sysSleepIntervalMinutes = CInt(System.Configuration.ConfigurationSettings.AppSettings("SleepIntervalMinutes"))
        Else
            sysSleepIntervalMinutes = 10
        End If

        Dim smtp As New SmtpClient(System.Configuration.ConfigurationSettings.AppSettings("SmtpServer"))
        Dim mail As MailMessage = New MailMessage
        mail.From = New MailAddress(System.Configuration.ConfigurationSettings.AppSettings("MailFrom"))
        mail.To.Add(System.Configuration.ConfigurationSettings.AppSettings("MailTo"))
        mail.CC.Add(System.Configuration.ConfigurationSettings.AppSettings("MailCc"))

        Dim stringWrapper As String = System.Configuration.ConfigurationSettings.AppSettings("StringWrapper")
        Dim delimiter As String = System.Configuration.ConfigurationSettings.AppSettings("Delimiter")
        Dim header1 As String = System.Configuration.ConfigurationSettings.AppSettings("Header1")
        Dim header2 As String = System.Configuration.ConfigurationSettings.AppSettings("Header2")

        If IsNumeric(stringWrapper) Then
            stringWrapper = Chr(CInt(stringWrapper))
        End If

        Dim timeLimitReached As Boolean
        Dim i, rowCount As Integer
        Dim dirArchiveStr, dirDestinationStr, fileName As String
        Dim header, aptGroupCode, lineWrite As String
        Dim headerArr As String()
        Dim transDate As DateTime
        Dim sw As StreamWriter
        Dim file As FileInfo
        Dim dir As DirectoryInfo
        Dim destHash, fileHash As Hashtable

        Try
            ConsoleWriteLine("------>>> AptPositions: Main Begin ------>>>")

            ConsoleWriteLine("Database Server = " + dbServerStr)
            ConsoleWriteLine("Database = " + dbStr)

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn

            sqlQuery = "DECLARE @ADATE DATETIME" + ControlChars.NewLine
            sqlQuery += "SELECT @ADATE = GETDATE()" + ControlChars.NewLine
            sqlQuery += "EXEC spUtil_business_days_add @start_date=@ADATE, @num_days=-1, @date_format=101, @return_date=@ADATE"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                transDate = CDate(rsReader.GetString(0))
                Console.WriteLine("Previous business day = " + transDate.ToString("d"))
            End While
            rsReader.Close()

            timeLimitReached = False
            rowCount = 0
            While rowCount < rowCountLimit
                If dbConn.State() = ConnectionState.Closed Then
                    dbConn.Open()
                End If
                dbCommand = New SqlCommand
                dbCommand.Connection = dbConn

                sqlQuery = "SELECT COUNT(*) FROM SQL03P.qmetrix.dbo.tblBenchmarkSecurity" + ControlChars.NewLine
                sqlQuery += " WHERE TransDate = '" + transDate.ToString("d") + "'"
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                While rsReader.Read()
                    rowCount = rsReader.GetInt32(0)
                    Console.WriteLine("Found " + CStr(rowCount) + " rows")
                    Console.WriteLine("Require " + CStr(rowCountLimit) + " rows")
                End While
                rsReader.Close()
                dbConn.Close()

                If rowCount < rowCountLimit Then
                    ConsoleWriteLine("Sleeping for " + CStr(sysSleepIntervalMinutes) + " minute(s)...")
                    Thread.Sleep(sysSleepIntervalMinutes * sysMinute)

                    If Now() > sleepLimitTime Then
                        ConsoleWriteLine("WARNING: Current time beyond " + sleepLimitTime.TimeOfDay().ToString().Substring(0, 8))
                        mail.Body = "Time is now " + Now().TimeOfDay().ToString().Substring(0, 8) + ControlChars.NewLine
                        mail.Body += "Current time beyond " + sleepLimitTime.TimeOfDay().ToString().Substring(0, 8) + ControlChars.NewLine
                        timeLimitReached = True
                        Exit While
                    End If
                Else
                    Exit While
                End If
            End While

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn
            dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute

            If timeLimitReached Then
                sqlQuery = "SELECT CONVERT(VARCHAR, MAX(x.TransDate), 101)" + ControlChars.NewLine
                sqlQuery += "  FROM (SELECT TransDate, COUNT(*) AS [RowCount]" + ControlChars.NewLine
                sqlQuery += "          FROM SQL03P.qmetrix.dbo.tblBenchmarkSecurity" + ControlChars.NewLine
                sqlQuery += "         WHERE TransDate >= DATEADD(wk, " + CStr(weeksBack) + ", GETDATE())" + ControlChars.NewLine
                sqlQuery += "         GROUP BY TransDate" + ControlChars.NewLine
                sqlQuery += "        HAVING COUNT(*) >= " + CStr(rowCountLimit) + ") x"
                ConsoleWriteLine()
                Console.WriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                While rsReader.Read()
                    If rsReader.IsDBNull(0) Then
                        ConsoleWriteLine("ERROR: No benchmark data found in qmetrix in previous " + CStr(Math.Abs(weeksBack)) + " weeks!")
                        ConsoleWriteLine("Terminating program...")
                        Exit Try
                    Else
                        transDate = CDate(rsReader.GetString(0))
                        Console.WriteLine("Benchmark data found on " + transDate.ToString("d"))
                    End If
                End While
                rsReader.Close()

                mail.Subject = "AptPositions Running Late"
                mail.Body += "Benchmark data will be retreived from " + transDate.ToString("d") + ControlChars.NewLine
                smtp.Send(mail)
            End If

            sqlQuery = "SELECT item_name, decode, notes FROM decode WHERE item_name LIKE 'APT_%' AND item_value = '~OUTPUT'"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            destHash = New Hashtable
            fileHash = New Hashtable
            While rsReader.Read()
                destHash.Add(rsReader.GetString(0), rsReader.GetString(1))
                fileHash.Add(rsReader.GetString(0), rsReader.GetString(2))
            End While
            rsReader.Close()

            sqlQuery = "SELECT decode FROM decode WHERE item_name = 'DIR' AND item_value = 'ARCHIVE' AND notes = 'QuantBatch'"
            ConsoleWriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                dirArchiveStr = rsReader.GetString(0)
            End While
            rsReader.Close()

            dir = New DirectoryInfo(dirArchiveStr)
            If Not dir.Exists() Then
                dir.Create()
            End If

            For Each aptGroupCode In destHash.Keys()
                sqlQuery = "EXEC apt_positions_get @APT_GROUP_CODE='" + aptGroupCode + "', @TRANS_DATE='" + transDate.ToString("d") + "'"
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()

                headerArr = header1.Split(",")
                header = ""
                For i = 0 To rsReader.FieldCount() - 1
                    If Not header.Equals("") Then
                        header += delimiter
                    End If
                    If i < headerArr.Length() Then
                        If Not CStr(headerArr.GetValue(i)).Equals("") Then
                            header += stringWrapper + headerArr.GetValue(i) + stringWrapper
                        Else
                            header += stringWrapper + rsReader.GetName(i) + stringWrapper
                        End If
                    Else
                        header += stringWrapper + rsReader.GetName(i) + stringWrapper
                    End If
                Next
                For i = rsReader.FieldCount() To headerArr.Length() - 1
                    If Not header.Equals("") Then
                        header += delimiter
                    End If
                    header += stringWrapper + headerArr.GetValue(i) + stringWrapper
                Next
                If Not header2.Equals("") Then
                    headerArr = header2.Split(",")
                    For i = 0 To headerArr.Length() - 1
                        If Not header.Equals("") Then
                            header += delimiter
                        End If
                        header += stringWrapper + headerArr.GetValue(i) + stringWrapper
                    Next
                End If

                fileName = Now().ToString("yyyyMMdd") + CStr(fileHash.Item(aptGroupCode))
                file = New FileInfo("C:\" + fileName)
                If file.Exists() Then
                    ConsoleWriteLine("Overwriting existing local file...")
                    file.Delete()
                End If
                file = Nothing
                ConsoleWriteLine("Writing C:\" + fileName)
                sw = New StreamWriter("C:\" + fileName)
                sw.WriteLine(header)

                While rsReader.Read()
                    lineWrite = ""
                    For i = 0 To rsReader.FieldCount() - 1
                        If Not rsReader.IsDBNull(i) Then
                            If Not lineWrite.Equals("") Then
                                lineWrite += delimiter
                            End If
                            Select Case rsReader.GetDataTypeName(i)
                                Case "bit"
                                    If rsReader(i).ToString().Equals("True") Then
                                        lineWrite += "1"
                                    Else
                                        lineWrite += "0"
                                    End If
                                Case "varchar"
                                    lineWrite += stringWrapper + rsReader.GetString(i) + stringWrapper
                                Case Else 'float, decimal
                                    lineWrite += rsReader(i).ToString()
                            End Select
                        Else
                            lineWrite += delimiter
                        End If
                    Next
                    sw.WriteLine(lineWrite)
                End While
                rsReader.Close()
                sw.Close()

                dirDestinationStr = destHash.Item(aptGroupCode)
                dir = New DirectoryInfo(dirDestinationStr)
                If Not dir.Exists() Then
                    dir.Create()
                End If

                file = New FileInfo("C:\" + fileName)
                If file.Exists() Then
                    ConsoleWriteLine("Copying to " + dirArchiveStr + fileName)
                    file.CopyTo(dirArchiveStr + fileName, True)
                    ConsoleWriteLine("Copying to " + dirDestinationStr + fileName)
                    file.CopyTo(dirDestinationStr + fileName, True)
                    file.Delete()
                Else
                    ConsoleWriteLine("ERROR: Output file " + file.FullName() + " not found!")
                    ConsoleWriteLine("Terminating program...")
                    Exit Try
                End If
            Next

            mail.Subject = "AptPositions Completed"
            mail.Body = "AptPositions completed at " + Now().TimeOfDay().ToString().Substring(0, 8) + ControlChars.NewLine
            mail.Body += "Benchmark data was retreived from " + transDate.ToString("d") + ControlChars.NewLine
            smtp.Send(mail)

            ConsoleWriteLine("<<<------ AptPositions: Main End <<<------")
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
