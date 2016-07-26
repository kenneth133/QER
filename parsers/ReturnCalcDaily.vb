Imports System.Data.SqlClient
Imports System.Threading
Imports Utility

Module ReturnCalcDaily

    Sub Main(ByVal args() As String)
        Dim dbServerStr As String = System.Configuration.ConfigurationSettings.AppSettings("DbServer")
        Dim dbStr As String = System.Configuration.ConfigurationSettings.AppSettings("Database")
        Dim dbConnStr As String = "Integrated Security=SSPI; Data Source=" + dbServerStr + "; Initial Catalog=" + dbStr
        Dim dbConn As SqlConnection = New SqlConnection(dbConnStr)

        Dim sqlQuery As String
        Dim dbCommand As SqlCommand
        Dim rsReader As SqlDataReader

        Dim sysSleepSeconds As Integer
        Dim dbTimeoutMinutes As Integer
        Dim sysSecond As Integer = 1000
        Dim dbMinute As Integer = 60

        If IsNumeric(System.Configuration.ConfigurationSettings.AppSettings("DbTimeoutMinutes")) Then
            dbTimeoutMinutes = CInt(System.Configuration.ConfigurationSettings.AppSettings("DbTimeoutMinutes"))
        Else
            dbTimeoutMinutes = 30
        End If

        Dim runDate, prevBusDate As DateTime
        If IsDate(System.Configuration.ConfigurationSettings.AppSettings("RunDate")) Then
            runDate = DateTime.Parse(System.Configuration.ConfigurationSettings.AppSettings("RunDate"))
        Else
            runDate = Now.Date()
        End If

        Dim rtnCalcId1, rtnCalcId2, i As Integer
        Dim rtnCalcIdArr As ArrayList

        Try
            If args.Length() < 2 Then
                Console.WriteLine("Error: Incorrect number of arguments passed")
                Console.WriteLine("Usage: ReturnCalcDaily.exe <secondsToSleep> <rtnCalcDailyId1> [rtnCalcDailyId2]")
                Exit Try
            Else
                If IsNumeric(args.GetValue(0)) Then
                    sysSleepSeconds = CInt(args.GetValue(0))
                Else
                    Console.WriteLine("Error: First parameter (secondsToSleep) must be a number")
                    Exit Try
                End If

                If IsNumeric(args.GetValue(1)) Then
                    rtnCalcId1 = CInt(args.GetValue(1))
                Else
                    Console.WriteLine("Error: Second parameter (rtnCalcDailyId1) must be a number")
                    Exit Try
                End If

                If args.Length() >= 3 Then
                    If IsNumeric(args.GetValue(2)) Then
                        rtnCalcId2 = CInt(args.GetValue(2))
                    Else
                        Console.WriteLine("Error: Third parameter (rtnCalcDailyId2) must be a number")
                        Exit Try
                    End If
                Else
                    rtnCalcId2 = Nothing
                End If
            End If

            ConsoleWriteLine("------>>> ReturnCalcDaily: Main Begin ------>>>")

            ConsoleWriteLine("Database Server = " + dbServerStr)
            ConsoleWriteLine("Database = " + dbStr)
            ConsoleWriteLine("returnCalcId1 = " + CStr(rtnCalcId1))
            If rtnCalcId2 = Nothing Then
                ConsoleWriteLine("returnCalcId2 = NULL")
            Else
                ConsoleWriteLine("returnCalcId2 = " + CStr(rtnCalcId2))
            End If
            ConsoleWriteLine("sleepSeconds = " + CStr(sysSleepSeconds))
            ConsoleWriteLine()

            If sysSleepSeconds > 0 Then
                ConsoleWriteLine("Sleeping for " + CStr(sysSleepSeconds) + " second(s)...")
                Thread.Sleep(sysSleepSeconds * sysSecond)
                ConsoleWriteLine()
            End If

            If dbConn.State() = ConnectionState.Closed Then
                dbConn.Open()
            End If
            dbCommand = New SqlCommand
            dbCommand.Connection = dbConn
            dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute

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

            sqlQuery = "SELECT return_calc_daily_id" + ControlChars.NewLine
            sqlQuery += "  FROM return_calc_params_daily" + ControlChars.NewLine
            If rtnCalcId2 = Nothing Then
                sqlQuery += " WHERE return_calc_daily_id >= " + CStr(rtnCalcId1) + ControlChars.NewLine
            Else
                If rtnCalcId1 <= rtnCalcId2 Then
                    sqlQuery += " WHERE return_calc_daily_id BETWEEN " + CStr(rtnCalcId1) + " AND " + CStr(rtnCalcId2) + ControlChars.NewLine
                Else
                    sqlQuery += " WHERE return_calc_daily_id BETWEEN " + CStr(rtnCalcId2) + " AND " + CStr(rtnCalcId1) + ControlChars.NewLine
                End If
            End If
            sqlQuery += " ORDER BY return_calc_daily_id"
            ConsoleWriteLine()
            Console.WriteLine(sqlQuery)
            dbCommand.CommandText = sqlQuery
            rsReader = dbCommand.ExecuteReader()
            rtnCalcIdArr = New ArrayList()
            While rsReader.Read()
                rtnCalcIdArr.Add(CInt(rsReader.GetInt32(0)))
            End While
            rsReader.Close()
            dbConn.Close()

            For Each i In rtnCalcIdArr
                dbConn.Open()
                dbCommand = New SqlCommand
                dbCommand.Connection = dbConn
                dbCommand.CommandTimeout = dbTimeoutMinutes * dbMinute

                sqlQuery = "EXEC return_calc_daily @BDATE='" + prevBusDate.ToString("d") + "', @RETURN_CALC_DAILY_ID=" + CStr(i)
                ConsoleWriteLine(sqlQuery)
                dbCommand.CommandText = sqlQuery
                dbCommand.ExecuteNonQuery()
                dbConn.Close()
            Next

            ConsoleWriteLine("<<<------ ReturnCalcDaily: Main End <<<------")
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
