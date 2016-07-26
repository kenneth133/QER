Imports System.Data.SqlClient
Imports System.IO

Public Module Library

    Public Sub ConsoleWrite()
        Console.Write(Now().TimeOfDay().ToString().Substring(0, 8))
    End Sub

    Public Sub ConsoleWrite(ByVal output As String)
        Console.Write(Now().TimeOfDay().ToString().Substring(0, 8) + " " + output)
    End Sub

    Public Sub ConsoleWriteLine()
        Console.WriteLine(Now().TimeOfDay().ToString().Substring(0, 8))
    End Sub

    Public Sub ConsoleWriteLine(ByVal output As String)
        Console.WriteLine(Now().TimeOfDay().ToString().Substring(0, 8) + " " + output)
    End Sub

    Public Function BcpConfirmRowCount(ByVal bcpBatFile As String) As Boolean
        Dim srBat, srBcp As StreamReader
        Dim fileHash, tableHash As Hashtable
        Dim tableNames As ArrayList
        Dim lineArr As String()
        Dim s, tableName, bcpFileName As String
        Dim i, lineCount As Integer
        Dim allGood As Boolean

        Dim dbServerStr As String = System.Configuration.ConfigurationSettings.AppSettings("DbServer")
        Dim dbStr As String = System.Configuration.ConfigurationSettings.AppSettings("Database")
        Dim dbConnStr As String = "Integrated Security=SSPI; Data Source=" + dbServerStr + "; Initial Catalog=" + dbStr
        Dim dbConn As SqlConnection = New SqlConnection(dbConnStr)

        Dim dbCommand As SqlCommand
        Dim rsReader As SqlDataReader
        Dim sqlQuery As String

        Try
            srBat = New StreamReader(bcpBatFile)
            fileHash = New Hashtable
            While srBat.Peek() >= 0
                lineArr = ChangeDelimiter(srBat.ReadLine(), " ", ",").Split(",")

                tableName = Trim(lineArr.GetValue(1))
                If tableName.StartsWith(ControlChars.Quote) And tableName.EndsWith(ControlChars.Quote) Then
                    tableName = tableName.Substring(1, tableName.Length() - 2)
                End If
                tableName = Trim(tableName)

                bcpFileName = Trim(lineArr.GetValue(3))
                If bcpFileName.StartsWith(ControlChars.Quote) And bcpFileName.EndsWith(ControlChars.Quote) Then
                    bcpFileName = bcpFileName.Substring(1, bcpFileName.Length() - 2)
                End If
                bcpFileName = Trim(bcpFileName)

                If Not fileHash.ContainsKey(tableName) Then
                    fileHash.Add(tableName, 0)
                End If

                srBcp = New StreamReader(bcpFileName)
                lineCount = 0
                While srBcp.Peek() >= 0
                    lineCount += 1
                    srBcp.ReadLine()
                End While
                srBcp.Close()

                lineCount += fileHash.Item(tableName)
                fileHash.Item(tableName) = lineCount
            End While
            srBat.Close()

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn

            tableNames = New ArrayList(fileHash.Keys())
            tableHash = New Hashtable
            For Each s In tableNames
                sqlQuery = "SELECT COUNT(*) FROM " + s
                dbCommand.CommandText = sqlQuery
                rsReader = dbCommand.ExecuteReader()
                While rsReader.Read()
                    tableHash.Add(s, rsReader.GetInt32(0))
                End While
                rsReader.Close()
            Next

            allGood = True
            For Each s In tableNames
                If fileHash.Item(s) <> tableHash.Item(s) Then
                    allGood = False
                    Exit For
                End If
            Next

            Return allGood
        Catch exSQL As SqlException
            Console.WriteLine(exSQL.ToString())
        Catch ex As Exception
            Console.WriteLine(ex.ToString())
        End Try
    End Function

    Public Sub GenerateBcpFiles(ByRef files As ArrayList, ByVal filter As String(), Optional ByVal numHeadLines As Integer = 0)
        Dim fileTemp As FileInfo
        Dim sr As StreamReader
        Dim sw As StreamWriter
        Dim lineWrite, strTemp, f As String
        Dim lineArr As String() = Nothing

        For Each fileTemp In files
            sr = New StreamReader(fileTemp.DirectoryName() + "\" + fileTemp.Name())
            For i As Integer = 1 To numHeadLines
                If sr.Peek() >= 0 Then
                    sr.ReadLine()
                End If
            Next

            lineArr = fileTemp.Name().Split(".")
            strTemp = fileTemp.DirectoryName() + "\"
            For j As Integer = 0 To lineArr.Length() - 2
                strTemp += lineArr.GetValue(j)
            Next
            strTemp += ".BCP"

            sw = New StreamWriter(strTemp)
            While sr.Peek() >= 0
                lineArr = ChangeDelimiter(sr.ReadLine(), ",", "|").Split("|")
                lineWrite = ""
                For Each strTemp In lineArr
                    strTemp = Trim(strTemp)
                    If strTemp.StartsWith(ControlChars.Quote) And strTemp.EndsWith(ControlChars.Quote) Then
                        strTemp = strTemp.Substring(1, strTemp.Length() - 2)
                    End If
                    strTemp = Trim(strTemp)
                    For Each f In filter
                        If UCase(strTemp).Equals(UCase(f)) Then
                            strTemp = ""
                        End If
                    Next
                    If lineWrite.Length = 0 Then
                        lineWrite += strTemp
                    Else
                        lineWrite += "|" + strTemp
                    End If
                Next
                sw.WriteLine(lineWrite)
            End While

            sw.Close()
            sr.Close()
        Next
    End Sub

    Public Function ChangeDelimiter(ByVal aLine As String, ByVal oldLimiter As String, ByVal newLimiter As String) As String
        Dim newLine As String = ""
        Dim inQuote As Boolean = False
        Dim i As Integer

        For i = 0 To aLine.Length() - 1
            If CStr(aLine.Chars(i)).Equals(ControlChars.Quote) Then
                inQuote = Not inQuote
            End If

            If CStr(aLine.Chars(i)).Equals(oldLimiter) And Not inQuote Then
                newLine += newLimiter
            Else
                newLine += CStr(aLine.Chars(i))
            End If
        Next

        Return newLine
    End Function

    Public Function RemoveE(ByVal sciNum As String) As String
        Dim eCounter, i, j As Integer
        Dim isNegative As Boolean = False

        If IsNumeric(sciNum) Then
            sciNum = CStr(CDbl(sciNum))
        Else
            GoTo GoReturn
        End If

        If sciNum.Contains("E") Then
            eCounter = CInt(sciNum.Split("E").GetValue(1))
            sciNum = sciNum.Split("E").GetValue(0)

            If sciNum < 0 Then
                isNegative = True
                sciNum = sciNum.Replace("-", "")
            End If

            If eCounter < 0 Then
                sciNum = sciNum.Replace(".", "")
                For i = 0 To Math.Abs(eCounter) - 2
                    sciNum = "0" + sciNum
                Next
                sciNum = "0." + sciNum
            Else
                j = eCounter - CStr(sciNum.Split(".").GetValue(1)).Length()
                For i = 0 To j - 1
                    sciNum = sciNum + "0"
                Next
                sciNum = sciNum.Replace(".", "")
            End If

            If isNegative Then
                sciNum = "-" + sciNum
            End If
        End If
GoReturn:
        Return sciNum
    End Function

    Public Function IdentifiersGet(ByVal lineArr As ArrayList, ByVal hdrNmIdxHash As Hashtable, ByVal delimiter As String) As String
        Dim rtnStr As String

        If hdrNmIdxHash.ContainsKey("MQAID") Then
            rtnStr = lineArr.Item(hdrNmIdxHash.Item("MQAID")) + delimiter
        Else
            rtnStr = delimiter
        End If
        If hdrNmIdxHash.ContainsKey("TICKER") Then
            rtnStr += lineArr.Item(hdrNmIdxHash.Item("TICKER")) + delimiter
        Else
            rtnStr += delimiter
        End If
        If hdrNmIdxHash.ContainsKey("CUSIP") Then
            rtnStr += lineArr.Item(hdrNmIdxHash.Item("CUSIP")) + delimiter
        Else
            rtnStr += delimiter
        End If
        If hdrNmIdxHash.ContainsKey("SEDOL") Then
            rtnStr += lineArr.Item(hdrNmIdxHash.Item("SEDOL")) + delimiter
        Else
            rtnStr += delimiter
        End If
        If hdrNmIdxHash.ContainsKey("ISIN") Then
            rtnStr += lineArr.Item(hdrNmIdxHash.Item("ISIN")) + delimiter
        Else
            rtnStr += delimiter
        End If
        If hdrNmIdxHash.ContainsKey("GVKEY") AndAlso IsNumeric(lineArr.Item(hdrNmIdxHash.Item("GVKEY"))) Then
            rtnStr += CInt(lineArr.Item(hdrNmIdxHash.Item("GVKEY")))
        End If

        Return rtnStr
    End Function

    Public Function Date112to101(ByVal date112 As String) As String
        Dim date101 As String

        If date112.Length() >= 8 Then
            date101 = date112.Substring(4, 2) + "/" + date112.Substring(6, 2) + "/" + date112.Substring(0, 4)
        Else
            date101 = date112
        End If

        Return date101
    End Function

    Public Function Date101to112(ByVal date101 As String) As String
        Dim date112 As String

        If date101.Length() >= 10 Then
            date112 = date101.Substring(6, 4) + date101.Substring(0, 2) + date101.Substring(3, 2)
        Else
            date112 = date101
        End If

        Return date112
    End Function

    Public Sub ArchiveTempDir(ByVal dirTempStr As String, ByVal dirArchiveStr As String)
        Dim fileArr As ArrayList
        Dim file As FileInfo
        Dim dirTemp, dirArchive As DirectoryInfo

        dirTemp = New DirectoryInfo(dirTempStr)
        fileArr = New ArrayList(dirTemp.GetFiles())
        If fileArr.Count() > 0 Then
            dirArchive = New DirectoryInfo(dirArchiveStr + dirTempStr.Split("\").GetValue(1) + "\")
            If Not dirArchive.Exists() Then
                dirArchive.Create()
            End If

            For Each file In fileArr
                file.CopyTo(dirArchiveStr + dirTempStr.Split("\").GetValue(1) + "\" + file.Name(), True)
                file.Delete()
            Next
        End If
        dirTemp.Delete()
    End Sub

End Module
