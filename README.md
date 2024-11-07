# BackupFiles
Backup files



ALTER PROCEDURE [dbo].[SP_Insert_Batchwise_data]
    @Date date,
    @CompanyCode nvarchar(100),
    @PlantCode nvarchar(100),
    @LineCode varchar(100),
    @MachineCode varchar(100)
AS
BEGIN
    SET NOCOUNT ON;

    -- Declare required variables
    DECLARE @FromDate datetime, @ToDate datetime, @BatchCode nvarchar(150), @MTTR int;

    -- Common Table Expressions (CTEs) to simplify data preparation
    WITH RawData AS (
        SELECT 
            Shift_Id, Variant_code, Batch_code, Time_stamp, Machine_Code,
            OK_Parts, NOK_Parts, Rework_Parts, Machine_status
        FROM Rawtable WITH (NOLOCK)
        WHERE CompanyCode = @CompanyCode 
            AND PlantCode = @PlantCode 
            AND Line_Code = @LineCode
            AND Machine_Code = @MachineCode 
            AND Date = @Date
    ),
    Temp11 AS (
        SELECT 
            Shift_Id AS Shift, Variant_code AS Variant, Batch_code AS Batch,
            ISNULL(LAG(Time_stamp) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp), Time_stamp) AS stime,
            Time_stamp AS etime, Machine_Code
        FROM RawData
    ),
    Temp12 AS (
        SELECT 
            Machine_code, MIN(stime) AS [start], MAX(etime) AS [end],
            Shift, Variant, Batch
        FROM Temp11
        GROUP BY Shift, Variant, Batch, Machine_code
    ),
    Temp AS (
        SELECT 
            Shift_Id AS Shift, Variant_code AS Variant, Batch_code AS Batch,
            CASE WHEN LAG(Batch_code) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) = Batch_code THEN '0' ELSE '1' END AS Batch_sts,
            ISNULL(LAG(Time_stamp) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp), Time_stamp) AS stime,
            Time_stamp AS etime,
            -- Calculate parts
            CASE 
                WHEN LAG(OK_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) IS NULL THEN 0
                WHEN OK_Parts > LAG(OK_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) THEN OK_Parts - LAG(OK_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp)
                ELSE OK_Parts
            END AS okparts,
            CASE 
                WHEN LAG(NOK_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) IS NULL THEN 0
                WHEN NOK_Parts > LAG(NOK_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) THEN NOK_Parts - LAG(NOK_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp)
                ELSE NOK_Parts
            END AS nokparts,
            CASE 
                WHEN LAG(Rework_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) IS NULL THEN 0
                WHEN Rework_Parts > LAG(Rework_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) THEN Rework_Parts - LAG(Rework_Parts) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp)
                ELSE Rework_Parts
            END AS reworkparts,
            CASE 
                WHEN Machine_status = '0' THEN 
                    CASE 
                        WHEN LAG(Machine_status) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) > 0 THEN 1
                        ELSE CASE WHEN LAG(Machine_status) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp) IS NULL AND Machine_status = 0 THEN 1 ELSE 0 END
                    END
                ELSE 0
            END AS stoppage,
            Machine_Code,
            -- Calculate time differences
            CASE WHEN Machine_status = '1' THEN DATEDIFF(SECOND, Time_stamp, LEAD(Time_stamp) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp)) ELSE 0 END AS uptime,
            CASE WHEN Machine_status = '0' THEN DATEDIFF(SECOND, Time_stamp, LEAD(Time_stamp) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp)) ELSE 0 END AS downtime,
            CASE WHEN Machine_status = '4' THEN DATEDIFF(SECOND, Time_stamp, LEAD(Time_stamp) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp)) ELSE 0 END AS breaktime,
            CASE WHEN Machine_status = '3' THEN DATEDIFF(SECOND, Time_stamp, LEAD(Time_stamp) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp)) ELSE 0 END AS losstime,
            CASE WHEN Machine_status = '5' THEN DATEDIFF(SECOND, Time_stamp, LEAD(Time_stamp) OVER (PARTITION BY Machine_Code ORDER BY Time_stamp)) ELSE 0 END AS plcDisconnect
        FROM RawData
    ),
    Temp1 AS (
        SELECT 
            Machine_code, 0 AS [Hour], MIN(stime) AS [start], MAX(etime) AS [end],
            Shift, Variant, Batch,
            SUM(okparts) AS okparts, ISNULL(SUM(nokparts), 0) AS nokparts,
            ISNULL(SUM(reworkparts), 0) AS reworkparts,
            SUM(stoppage) AS stoppage, SUM(uptime) / 60 AS uptime, SUM(downtime) / 60 AS downtime,
            SUM(breaktime) / 60 AS breaktime, SUM(losstime) / 60 AS losstime, SUM(plcDisconnect) / 60 AS plcDisconnect
        FROM Temp
        GROUP BY Shift, Variant, Batch, Machine_code
    ),
    Temp2 AS (
        SELECT 
            [start], [end], [Hour], Shift, a.Variant, b.VariantName, a.Machine_code, Batch,
            ISNULL(okparts, 0) AS okparts, ISNULL(nokparts, 0) AS nokparts, ISNULL(reworkparts, 0) AS reworkparts, stoppage,
            uptime, downtime, breaktime, losstime,
            CASE WHEN (uptime + downtime + losstime + breaktime) = 0 THEN 0 
                 ELSE (100 - ((downtime * 100) / (uptime + downtime + losstime + breaktime)))
            END AS Availability,
            CASE WHEN uptime IS NULL OR uptime = 0 THEN 0
                 ELSE CASE WHEN ((okparts + nokparts) * b.Ideal_cycletime) / (uptime * 60) * 100 > 100 THEN 100
                           ELSE ISNULL(((okparts + nokparts) * b.Ideal_cycletime) / (uptime * 60) * 100, 0)
                      END
            END AS Performance,
            CASE WHEN ISNULL(okparts, 0) = 0 THEN 0 ELSE (okparts * 100) / (okparts + ISNULL(nokparts, 0)) END AS Quality,
            plcDisconnect
        FROM Temp1 AS a
        LEFT JOIN tbl_MasterProduct AS b 
            ON a.Variant = b.Variant_Code COLLATE SQL_Latin1_General_CP1_CI_AS
           AND b.CompanyCode = @CompanyCode 
           AND b.PlantCode = @PlantCode 
           AND b.Machine_Code = @MachineCode 
           AND b.Line_Code = @LineCode
    )

    -- Calculate MTTR
    SELECT @MTTR = ISNULL(SUM(DATEDIFF(SECOND, a.start_time, a.End_Time)) / 60, 0)
    FROM MachineAlarm WITH (NOLOCK) a
    JOIN AlarmTable_Setting WITH (NOLOCK) b 
        ON a.Alarm_ID = b.Alarm_ID
       AND a.Line_Code = @LineCode 
       AND a.Machine_Code = @MachineCode 
       AND a.CompanyCode = @CompanyCode 
       AND a.PlantCode = @PlantCode
    WHERE a.Date BETWEEN @FromDate AND @ToDate;

    -- Update or Insert into tbl_batchwise_live_data
    IF EXISTS (SELECT 1 FROM tbl_batchwise_live_data WHERE Date = @Date AND Machine_code = @MachineCode)
    BEGIN
        UPDATE tbl_batchwise_live_data
        SET OK_Parts = a.okparts, NOK_Parts = a.nokparts, Start_time = [start], End_time = [end],
            No_of_stopage = a.stoppage, Uptime = a.uptime, Downtime = a.downtime, Breaktime = a.breaktime,
            Losstime = a.losstime, Availability = a.Availability, Performance = a.Performance, Quality = a.Quality,
            OEE = ((a.Availability * a.Performance * a.Quality) / 10000), MTTR = @MTTR, Batch = a.Batch,
            Shift_id = a.Shift, Variant = a.VariantName, DLK_to_PLC_Disconnect_time = a.plcDisconnect,
            Rework_Parts = a.reworkparts
        FROM Temp2 a
        WHERE a.Machine_code = tbl_batchwise_live_data.Machine_code;
    END
    ELSE
    BEGIN
        INSERT INTO tbl_batchwise_live_data
            (Machine_code, OK_Parts, NOK_Parts, Start_time, End_time, No_of_stopage, Uptime, Downtime, Breaktime, 
             Losstime, Availability, Performance, Quality, OEE, MTTR, Batch, Shift_id, Variant, Date, 
             DLK_to_PLC_Disconnect_time, Rework_Parts)
        SELECT 
            Machine_code, okparts, nokparts, [start], [end], stoppage, uptime, downtime, breaktime, 
            losstime, Availability, Performance, Quality, 
            (Availability * Performance * Quality) / 10000, @MTTR, Batch, Shift, VariantName, 
            @Date, plcDisconnect, reworkparts
        FROM Temp2;
    END
END;
