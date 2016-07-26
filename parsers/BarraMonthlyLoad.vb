Imports System.Data.SqlClient
Imports System.IO
Imports System.Net
Imports System.Net.Mail
Imports Utility

Module BarraMonthlyLoad

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

        Dim dbMinute As Integer = 60
        Dim dbTimeoutMinutes As Integer
        If IsNumeric(System.Configuration.ConfigurationSettings.AppSettings("DbTimeoutMinutes")) Then
            dbTimeoutMinutes = CInt(System.Configuration.ConfigurationSettings.AppSettings("DbTimeoutMinutes"))
        Else
            dbTimeoutMinutes = 20
        End If

        Dim reload As Boolean = "TRUE".Equals(UCase(System.Configuration.ConfigurationSettings.AppSettings("Reload")))
        Dim yyyymm As String = System.Configuration.ConfigurationSettings.AppSettings("yyyymm")

        Dim smtp As New SmtpClient(System.Configuration.ConfigurationSettings.AppSettings("SmtpServer"))
        Dim mail As New MailMessage()
        If UCase(dbServerStr).EndsWith("P") Then
            mail.From = New MailAddress(System.Configuration.ConfigurationSettings.AppSettings("MailFromTidalProd"))
        Else
            mail.From = New MailAddress(System.Configuration.ConfigurationSettings.AppSettings("MailFromTidalDev"))
        End If
        mail.To.Add(System.Configuration.ConfigurationSettings.AppSettings("MailTo"))
        mail.CC.Add(System.Configuration.ConfigurationSettings.AppSettings("MailCc"))

        Dim host As String = System.Configuration.ConfigurationSettings.AppSettings("FtpHost")
        Dim user As String = System.Configuration.ConfigurationSettings.AppSettings("FtpUser")
        Dim pw As String = System.Configuration.ConfigurationSettings.AppSettings("FtpPw")

        Dim rowCountOkay As Boolean
        Dim i As Integer
        Dim dirArchiveStr, dirTempStr, dirUse3LdStr, dirUse3LmStr, dirUse3SdStr, dirUse3SmStr, dirListing As String
        Dim batFile, lineWrite, name, s, str, aString, yymm As String
        Dim covDate, rskDate As DateTime
        Dim arr, fileArr, dirArr, lineArr As ArrayList
        Dim sr As StreamReader
        Dim sw As StreamWriter
        Dim file As FileInfo
        Dim dir, dirArchive As DirectoryInfo
        Dim ht As Hashtable
        Dim req As FtpWebRequest
        Dim res As FtpWebResponse
        Dim client As WebClient

        Try
            ConsoleWriteLine("------>>> BarraMonthlyLoad: Main Begin ------>>>")

            Console.WriteLine("Database Server = " + dbServerStr)
            Console.WriteLine("Database = " + dbStr)
            Console.WriteLine("Reload = " + UCase(reload.ToString()))

            ht = New Hashtable
            ht.Add("barra_covariance", False)
            ht.Add("barra_factor_returns", False)
            ht.Add("barra_risk", False)
            ht.Add("barra_risk2", False)

            If yyyymm.Equals("") Then
                yyyymm = Now().AddMonths(-1).ToString("yyyyMM")
            End If

            yymm = yyyymm.Substring(2, 4)

            If Not reload Then
                ConsoleWriteLine("------>>> BarraMonthlyLoad: CheckData Begin ------>>>")

                If dbConn.State() = ConnectionState.Closed Then
                    dbConn.Open()
                End If
                dbCommand = New SqlCommand
                dbCommand.Connection = dbConn

                arr = New ArrayList(ht.Keys())
                For Each s In arr
                    sqlQuery = "SELECT CONVERT(varchar, month_end_dt, 101), COUNT(*) FROM " + s + ControlChars.NewLine
                    sqlQuery += " WHERE DATEPART(yy, month_end_dt) = " + yyyymm.Substring(0, 4) + ControlChars.NewLine
                    sqlQuery += "   AND DATEPART(mm, month_end_dt) = " + yyyymm.Substring(4, 2) + ControlChars.NewLine
                    sqlQuery += " GROUP BY month_end_dt"
                    Console.WriteLine()
                    Console.WriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    rsReader = dbCommand.ExecuteReader()
                    If rsReader.HasRows() Then
                        ht.Item(s) = True
                        While rsReader.Read()
                            Console.WriteLine()
                            Console.WriteLine(CStr(rsReader.GetInt32(1)) + " row(s) found on " + rsReader.GetString(0))
                        End While
                    Else
                        Console.WriteLine()
                        Console.WriteLine("No rows found for " + CDate(yyyymm.Substring(4, 2) + "/01/" + yyyymm.Substring(0, 4)).ToString("MMMM yyyy"))
                    End If
                    rsReader.Close()
                Next

                dbConn.Close()
                Console.WriteLine()
                ConsoleWriteLine("<<<------ BarraMonthlyLoad: CheckData End <<<------")

                If Not ht.ContainsValue(False) Then
                    Console.WriteLine("Barra data has been loaded previously")
                    Console.WriteLine("Exiting program...")
                    GoTo MainExit
                End If
            End If

            ConsoleWriteLine("------>>> BarraMonthlyLoad: Initialization Begin ------>>>")

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn

            sqlQuery = "SELECT code, decode FROM decode" + ControlChars.NewLine
            sqlQuery += "WHERE item = 'DIR' AND code IN ('ARCHIVE','BARRA_USE3L_DAILY','BARRA_USE3L_MONTHLY','BARRA_USE3S_DAILY','BARRA_USE3S_MONTHLY')"
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                If rsReader.GetString(0).Equals("ARCHIVE") Then
                    dirArchiveStr = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("BARRA_USE3L_DAILY") Then
                    dirUse3LdStr = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("BARRA_USE3L_MONTHLY") Then
                    dirUse3LmStr = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("BARRA_USE3S_DAILY") Then
                    dirUse3SdStr = rsReader.GetString(1)
                ElseIf rsReader.GetString(0).Equals("BARRA_USE3S_MONTHLY") Then
                    dirUse3SmStr = rsReader.GetString(1)
                End If
            End While
            rsReader.Close()

            sqlQuery = "SELECT convert(varchar, getdate(), 112) + '_' + convert(varchar, getdate(), 108)"
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            While rsReader.Read()
                dirTempStr = "C:\temp_BarraMonthlyLoad_" + rsReader.GetString(0).Replace(":", "") + "\"
            End While
            rsReader.Close()
            dbConn.Close()

            dirArchive = New DirectoryInfo(dirArchiveStr)
            dir = New DirectoryInfo(dirTempStr)
            If Not dir.Exists() Then
                dir.Create()
            End If

            ConsoleWriteLine("<<<------ BarraMonthlyLoad: Initialization End <<<------")

            If Not reload Then
                ConsoleWriteLine("------>>> BarraMonthlyLoad: CheckArchive Begin ------>>>")

                For Each s In New ArrayList(ht.Keys())
                    If Not ht.Item(s) Then
                        If s.Contains("covariance") Then
                            str = dirUse3LmStr + "USE3L" + yymm + ".COV"
                        ElseIf s.Contains("factor_returns") Then
                            str = dirUse3LmStr + "USE3L" + yymm + ".FRT"
                        ElseIf s.Contains("risk") Then
                            str = dirUse3LmStr + "USE3L" + yymm + ".RSK"
                        End If

                        file = New FileInfo(str)
                        If file.Exists Then
                            Console.WriteLine("Found " + file.FullName())
                            Console.WriteLine(" Copy to " + dirTempStr)
                            file.CopyTo(dirTempStr + file.Name(), True)
                            ht.Item(s) = True
                        Else
                            Console.WriteLine(file.FullName() + " not found")
                        End If
                    End If
                Next

                ConsoleWriteLine("<<<------ BarraMonthlyLoad: CheckArchive End <<<------")
            End If

            If ht.ContainsValue(False) Then
                ConsoleWriteLine("------>>> BarraMonthlyLoad: GetFiles Begin ------>>>")

                req = FtpWebRequest.Create(host)
                req.Credentials = New NetworkCredential(user, pw)
                req.Method = WebRequestMethods.Ftp.ListDirectoryDetails
                res = req.GetResponse()
                sr = New StreamReader(res.GetResponseStream())
                dirListing = sr.ReadToEnd()
                sr.Close()
                res.Close()

                fileArr = New ArrayList
                dirArr = New ArrayList

                For Each s In dirListing.Split(ControlChars.Lf)
                    arr = New ArrayList(s.Trim().Split(" "))
                    If arr.Count() >= 3 AndAlso Not CStr(arr.Item(arr.Count() - 1)).Equals(".") AndAlso Not CStr(arr.Item(arr.Count() - 1)).Equals("..") Then
                        name = CStr(arr.Item(arr.Count() - 1))
                        If CStr(arr.Item(0)).StartsWith("d") Then
                            dirArr.Add(name)
                        Else
                            If name.StartsWith("USE3L" + yymm) And name.Length() = 13 Then
                                If (name.EndsWith(".COV") And Not ht.Item("barra_covariance")) Or _
                                   (name.EndsWith(".FRT") And Not ht.Item("barra_factor_returns")) Or _
                                   (name.EndsWith(".RSK") And Not (ht.Item("barra_risk") And ht.Item("barra_risk2"))) Then
                                    fileArr.Add(name)
                                End If
                            Else
                                str = Nothing
                                If name.StartsWith("USE3L") And name.Length() = 15 Then
                                    str = dirUse3LdStr
                                ElseIf name.StartsWith("USE3L") And name.Length() = 13 Then
                                    str = dirUse3LmStr
                                ElseIf name.StartsWith("USE3S") And name.Length() = 15 Then
                                    str = dirUse3SdStr
                                ElseIf name.StartsWith("USE3S") And name.Length() = 13 Then
                                    str = dirUse3SmStr
                                End If

                                If Not str Is Nothing Then
                                    file = New FileInfo(str + name)
                                    If Not file.Exists Then
                                        fileArr.Add(name)
                                    End If
                                End If
                            End If
                        End If
                    End If
                Next

                For Each aString In dirArr
                    req = FtpWebRequest.Create(host + "/" + aString)
                    req.Credentials = New NetworkCredential(user, pw)
                    req.Method = WebRequestMethods.Ftp.ListDirectoryDetails
                    res = req.GetResponse()
                    sr = New StreamReader(res.GetResponseStream())
                    dirListing = sr.ReadToEnd()
                    sr.Close()
                    res.Close()

                    For Each s In dirListing.Split(ControlChars.Lf)
                        arr = New ArrayList(s.Trim().Split(" "))
                        If arr.Count() >= 3 AndAlso Not CStr(arr.Item(arr.Count() - 1)).Equals(".") AndAlso Not CStr(arr.Item(arr.Count() - 1)).Equals("..") AndAlso Not CStr(arr.Item(0)).StartsWith("d") Then
                            name = CStr(arr.Item(arr.Count() - 1))
                            If name.StartsWith("USE3L" + yymm) And name.Length() = 13 Then
                                If (name.EndsWith(".COV") And Not ht.Item("barra_covariance")) Or _
                                   (name.EndsWith(".FRT") And Not ht.Item("barra_factor_returns")) Or _
                                   (name.EndsWith(".RSK") And Not (ht.Item("barra_risk") And ht.Item("barra_risk2"))) Then
                                    fileArr.Add(aString + "/" + name)
                                End If
                            Else
                                str = Nothing
                                If name.StartsWith("USE3L") And name.Length() = 15 Then
                                    str = dirUse3LdStr
                                ElseIf name.StartsWith("USE3L") And name.Length() = 13 Then
                                    str = dirUse3LmStr
                                ElseIf name.StartsWith("USE3S") And name.Length() = 15 Then
                                    str = dirUse3SdStr
                                ElseIf name.StartsWith("USE3S") And name.Length() = 13 Then
                                    str = dirUse3SmStr
                                End If

                                If Not str Is Nothing Then
                                    file = New FileInfo(str + name)
                                    If Not file.Exists Then
                                        fileArr.Add(aString + "/" + name)
                                    End If
                                End If
                            End If
                        End If
                    Next
                Next

                For Each name In fileArr
                    arr = New ArrayList(name.Split("/"))
                    s = arr.Item(arr.Count() - 1)
                    client = New WebClient
                    client.Credentials = New NetworkCredential(user, pw)
                    ConsoleWriteLine("Downloading " + host + "/" + name)
                    client.DownloadFile(host + "/" + name, dirTempStr + s)
                Next

                ConsoleWriteLine("<<<------ BarraMonthlyLoad: GetFiles End <<<------")
            End If

            ConsoleWriteLine("------>>> BarraMonthlyLoad: ProcessFiles Begin ------>>>")
            fileArr = New ArrayList(dir.GetFiles("USE3*"))
            For Each file In fileArr
                If file.Name().StartsWith("USE3L") And file.Name().Length() = 13 Then
                    If file.Name().EndsWith(".COV") Or file.Name().EndsWith(".RSK") Or file.Name().EndsWith(".FRT") Then
                        sr = New StreamReader(file.FullName())

                        If file.Name().EndsWith(".COV") Then
                            If sr.Peek() Then
                                covDate = CDate(sr.ReadLine().Split(" ").GetValue(0))
                            End If
                        ElseIf file.Name().EndsWith(".RSK") Then
                            If sr.Peek() Then
                                rskDate = CDate(sr.ReadLine().Trim())
                            End If
                        End If

                        If sr.Peek() >= 0 Then
                            sr.ReadLine()
                            If file.Name().EndsWith(".COV") Or file.Name().EndsWith(".FRT") Then
                                sr.ReadLine()
                            End If
                        End If

                        ConsoleWriteLine("Writing " + file.Name().Split(".").GetValue(0) + "_" + file.Name().Split(".").GetValue(1) + ".BCP")
                        sw = New StreamWriter(file.DirectoryName() + "\" + CStr(file.Name().Split(".").GetValue(0)) + "_" + CStr(file.Name().Split(".").GetValue(1)) + ".BCP")
                        While sr.Peek() >= 0
                            If file.Name().EndsWith(".COV") Then
                                lineWrite = sr.ReadLine()
                                lineWrite = lineWrite.Replace(" ", "")
                                lineWrite = lineWrite.Replace(ControlChars.Quote, "")
                                str = ","
                            Else
                                lineWrite = ""
                                str = "|"
                                lineArr = New ArrayList(ChangeDelimiter(sr.ReadLine(), ",", str).Split(str))
                                For i = 0 To lineArr.Count() - 1
                                    s = lineArr.Item(i)

                                    s = Trim(s)
                                    If s.StartsWith(ControlChars.Quote) And s.EndsWith(ControlChars.Quote) Then
                                        s = s.Substring(1, s.Length() - 2)
                                    End If
                                    s = Trim(s)

                                    If UCase(s).Equals("NULL") Then
                                        s = ""
                                    End If

                                    If file.Name().EndsWith(".FRT") Then
                                        If i = 0 Then
                                            s += "01"
                                        Else
                                            s = RemoveE(s)
                                        End If
                                    End If

                                    If lineWrite.Length() = 0 Then
                                        lineWrite += s
                                    Else
                                        lineWrite += str + s
                                    End If
                                Next
                            End If

                            If file.Name().EndsWith(".COV") Or file.Name().EndsWith(".FRT") Then
                                i = lineWrite.Split(str).Length()
                                While i < 69
                                    lineWrite += str
                                    i += 1
                                End While
                                While i > 69
                                    If Not lineWrite.EndsWith(str) Then
                                        Exit While
                                    End If
                                    lineWrite = lineWrite.Substring(0, lineWrite.Length() - 1)
                                    i -= 1
                                End While
                            End If

                            sw.WriteLine(lineWrite)
                        End While
                        sw.Close()
                        sr.Close()
                    End If

                    ConsoleWriteLine("Move " + file.Name() + " to " + dirUse3LmStr)
                    file.CopyTo(dirUse3LmStr + file.Name(), True)
                    file.Delete()
                ElseIf file.Name().StartsWith("USE3L") And file.Name().Length() = 15 Then
                    ConsoleWriteLine("Move " + file.Name() + " to " + dirUse3LdStr)
                    file.CopyTo(dirUse3LdStr + file.Name(), True)
                    file.Delete()
                ElseIf file.Name().StartsWith("USE3S") And file.Name().Length() = 13 Then
                    ConsoleWriteLine("Move " + file.Name() + " to " + dirUse3SmStr)
                    file.CopyTo(dirUse3SmStr + file.Name(), True)
                    file.Delete()
                ElseIf file.Name().StartsWith("USE3S") And file.Name().Length() = 15 Then
                    ConsoleWriteLine("Move " + file.Name() + " to " + dirUse3SdStr)
                    file.CopyTo(dirUse3SdStr + file.Name(), True)
                    file.Delete()
                End If
            Next
            ConsoleWriteLine("<<<------ BarraMonthlyLoad: ProcessFiles End <<<------")

            fileArr = New ArrayList(dir.GetFiles("*.BCP"))
            If fileArr.Count() <= 0 Then
                ConsoleWriteLine("No files to load")
                GoTo ArchiveFiles
            End If

            ConsoleWriteLine("------>>> BarraMonthlyLoad: DbLoad Begin ------>>>")
            batFile = dirTempStr + "bcpbcp.bat"
            ConsoleWriteLine("Writing " + batFile)
            sw = New StreamWriter(batFile)
            sqlQuery = ""
            str = ""
            For Each file In fileArr
                If file.Name().EndsWith("_COV.BCP") Then
                    lineWrite = "bcp QER..barra_covariance_staging in "
                    If sqlQuery.Equals("") Then
                        sqlQuery += "DELETE barra_covariance_staging"
                        str += "EXEC barra_covariance_load @MONTH_END_DT='" + covDate.ToString("d") + "'"
                    Else
                        sqlQuery += ControlChars.NewLine + "DELETE barra_covariance_staging"
                        str += "|" + "EXEC barra_covariance_load @MONTH_END_DT='" + covDate.ToString("d") + "'"
                    End If
                ElseIf file.Name().EndsWith("_FRT.BCP") Then
                    lineWrite = "bcp QER..barra_factor_returns_staging in "
                    If sqlQuery.Equals("") Then
                        sqlQuery += "DELETE barra_factor_returns_staging"
                        str += "EXEC barra_factor_returns_load"
                    Else
                        sqlQuery += ControlChars.NewLine + "DELETE barra_factor_returns_staging"
                        str += "|" + "EXEC barra_factor_returns_load"
                    End If
                ElseIf file.Name().EndsWith("_RSK.BCP") Then
                    lineWrite = "bcp QER..barra_risk_staging in "
                    If sqlQuery.Equals("") Then
                        sqlQuery += "DELETE barra_risk_staging"
                        str += "EXEC barra_risk_load @MONTH_END_DT='" + rskDate.ToString("d") + "'"
                    Else
                        sqlQuery += ControlChars.NewLine + "DELETE barra_risk_staging"
                        str += "|" + "EXEC barra_risk_load @MONTH_END_DT='" + rskDate.ToString("d") + "'"
                    End If
                    str += "|" + "EXEC barra_risk2_load @MONTH_END_DT='" + rskDate.ToString("d") + "'"
                End If

                If file.Name().EndsWith("_COV.BCP") Then
                    lineWrite += file.FullName() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "," + ControlChars.Quote + " -T"
                Else
                    lineWrite += file.FullName() + " -S" + dbServerStr + " -c -t" + ControlChars.Quote + "|" + ControlChars.Quote + " -T"
                End If

                sw.WriteLine(lineWrite)
            Next
            sw.Close()

            If Not sqlQuery.Equals("") Then
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

                For Each sqlQuery In str.Split("|")
                    ConsoleWriteLine(sqlQuery)
                    dbCommand.CommandText = sqlQuery
                    dbCommand.ExecuteNonQuery()
                Next
                dbConn.Close()
            End If
            ConsoleWriteLine("<<<------ BarraMonthlyLoad: DbLoad End <<<------")

            ConsoleWriteLine("------>>> BarraMonthlyLoad: EmailNotify Begin ------>>>")
            mail.Subject = "Barra USE3L Monthly Data Has Been Loaded"
            mail.Body = "Data was loaded to " + dbStr + " database on " + dbServerStr + " for " + CDate(yyyymm.Substring(4, 2) + "/01/" + yyyymm.Substring(0, 4)).ToString("MMMM yyyy")
            smtp.Send(mail)
            ConsoleWriteLine("<<<------ BarraMonthlyLoad: EmailNotify End <<<------")
ArchiveFiles:
            ConsoleWriteLine("------>>> BarraMonthlyLoad: ArchiveFiles Begin ------>>>")
            If Not dirArchive.Exists() Then
                dirArchive.Create()
            End If
            ConsoleWriteLine("Archive Directory = " + dirArchiveStr)
            ArchiveTempDir(dirTempStr, dirArchiveStr)
            ConsoleWriteLine("<<<------ BarraMonthlyLoad: ArchiveFiles End <<<------")
MainExit:
            ConsoleWriteLine("<<<------ BarraMonthlyLoad: Main End <<<------")
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
