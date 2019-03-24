USE master
GO
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_maxsize_tempdb')
    EXEC ('CREATE PROCEDURE dbo.sp_maxsize_tempdb AS SELECT ''temporary holding procedure''')
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_maxsize_tempdb]
    @Action                         NVARCHAR(64)    = NULL,
    @LastServerwideCountTempFiles   INT             = NULL,
    @LastTempFilesDrive             NCHAR(1)        = NULL,
    @MBToReserve                    NUMERIC(10,1)   = NULL,
    @BlitzDatabaseName              NVARCHAR(256)   = NULL,
    @BlitzSchemaName                NVARCHAR(256)   = NULL,
    @BlitzTableName                 NVARCHAR(256)   = NULL, 
    @TargetInstanceName             NVARCHAR(256)   = NULL,
    @TargetLinkName                 NVARCHAR(256)   = NULL   
AS
-----------------------------------------------------------------------------------------------
-- 
-- name:    
--      sp_maxsize_tempdb - a stored procedure wrapper around a set of queries to come up with a
--                              recommended MAXSIZE value for tempdb.
--      
--      parameters 
--          @Action = 'VERIFY'      - check that expected tempdb locations are still accurate
--          @Action = 'RECOMMEND'   - only recommend new tempdb maxsize settings 
--          @Action = 'ALTER'       - run alter database commands to apply tempdb recommended maxsize settings
--
--          @TargetLinkName         - linked table linkname on the mgmt instance
--
-- syntax:
--      EXEC sp_maxsize_tempdb @Action = N'VERIFY', @LastServerwideCountTempFiles = 8, @LastTempFilesDrive = 'E', @MBToReserve = 5120, @BlitzDatabaseName = N'dbadatabase', @BlitzSchemaName = N'dbo', @BlitzTableName = N'BlitzResults', @TargetInstanceName = N'testdbserver', @TargetLinkName = N'testdbserver_linkname', @VerifyCode = 1 ;
--      EXEC sp_maxsize_tempdb @Action = N'RECOMMEND', @LastServerwideCountTempFiles = 8, @LastTempFilesDrive = 'E', @MBToReserve = 5120, @BlitzDatabaseName = N'dbadatabase', @BlitzSchemaName = N'dbo', @BlitzTableName = N'BlitzResults', @TargetInstanceName = N'testdbserver', @TargetLinkName = N'testdbserver_linkname';
--      EXEC sp_maxsize_tempdb @Action = N'Typographical Error';
--
-- dependencies:
--      1) a sqlagent job to run this via PowerShell daily
--      
--
-- updated:
--      -- Monday, March 18, 2019 12:33 PM
-- 

BEGIN
--SET NOCOUNT ON;
DECLARE @errormessage NVARCHAR(2048), @errornumber INT, @errorseverity INT, @errorstate INT, @errorline INT;
DECLARE @LargestTempfileSizeInMB NUMERIC(10,1);
DECLARE @TempFilesDrive NCHAR(1); 
DECLARE @InstanceCountTempFiles INT; 
DECLARE @MinMBFree NUMERIC(10,1);
DECLARE @AvgMBFree NUMERIC(10,1);
DECLARE @MaxMBFree NUMERIC(10,1);
DECLARE @TempdbFiles TABLE 
    (
    LogicalFileName NVARCHAR(40)
    );
DECLARE @MaxAvailablePerTempFile    NUMERIC(10,1);
DECLARE @RecommendedNewSize         NUMERIC(10,1); 
DECLARE @RecommendedNewSizeRounded  NUMERIC(10,1);
DECLARE @sql NVARCHAR(MAX);
DECLARE @paramlist  nvarchar(4000);  
    
IF (@Action = N'VERIFY')
BEGIN
    SELECT 'todo'
END

ELSE IF (@Action = N'RECOMMEND' OR @Action = N'ALTER')
BEGIN

    -- [1] -- get current max tempdb filesize for [c20] and percentage used (note cant easily get percentage over linked server.)    
    SELECT @sql =                                                                               
        'SELECT 
            @xLargestTempfileSizeInMB = MAX(CAST(size/128.0 AS NUMERIC(10,1))) 
        FROM ' + QUOTENAME(@TargetLinkName, N']') + '.tempdb.dbo.sysfiles
        WHERE groupid = 1' 
    
    SELECT 
        @paramlist = '@xLargestTempfileSizeInMB NUMERIC(10,1) OUTPUT'                                                                   
                                                                                                
    EXEC sp_executesql @sql, @paramlist, 
        @LargestTempfileSizeInMB OUTPUT;

    PRINT @LargestTempfileSizeInMB; -- debug

    -- [2a] -- get drivename for [c4]  
    SELECT @sql =                                                                               
        'SELECT @xTempFilesDrive = LEFT(filename, 1) 
        FROM ' + QUOTENAME(@TargetLinkName, N']') + '.tempdb.dbo.sysfiles
        WHERE groupid = 1    
        GROUP BY LEFT(filename, 1)'
    
    SELECT 
        @paramlist = '@xTempFilesDrive NCHAR(1) OUTPUT'                                                                   
                                                                                                
    EXEC sp_executesql @sql, @paramlist, 
        @TempFilesDrive OUTPUT;

    PRINT @TempFilesDrive;
    PRINT 'tempfiles are still on the same drive: ' + CASE WHEN @TempFilesDrive <> @LastTempFilesDrive THEN 'false (Warning! The drive that we expect to hold tempdb has changed)' ELSE 'true' END;

    
    -- [2b] to calculate [c13] take the count of nTempFiles, Compare to last known number of temp files, or rerun on ALL instances
    SELECT @sql =                                                                               
        'SELECT @xInstanceCountTempFiles = COUNT(*) 
        FROM ' + QUOTENAME(@TargetLinkName, N']') + '.tempdb.dbo.sysfiles
        WHERE groupid = 1    
        GROUP BY LEFT(filename, 1)'
    
    SELECT 
        @paramlist = '@xInstanceCountTempFiles INT OUTPUT'                                                                   
                                                                                                
    EXEC sp_executesql @sql, @paramlist, 
        @InstanceCountTempFiles OUTPUT;

    PRINT @InstanceCountTempFiles;
    PRINT 'serverwide tempfiles are all on this instance: ' + CASE WHEN @InstanceCountTempFiles <> @LastServerwideCountTempFiles THEN 'false (Warning! We need to count the tempfiles on all instances.)' ELSE 'true' END;

    -- [3] -- get latest montly trend on free space available on the drive [c4] and put the low point into [c6]
    SELECT @sql =                                                                               
        'SELECT  
            @xMinMBFree = MIN(CAST(SUBSTRING(Details, 1, CHARINDEX(''MB'',Details)-1) AS NUMERIC(10,1))),
            @xAvgMBFree = AVG(CAST(SUBSTRING(Details, 1, CHARINDEX(''MB'',Details)-1) AS NUMERIC(10,1))),
            @xMaxMBFree = MAX(CAST(SUBSTRING(Details, 1, CHARINDEX(''MB'',Details)-1) AS NUMERIC(10,1))) 
        FROM ' + QUOTENAME(@BlitzDatabaseName, N']') + '.' + QUOTENAME(@BlitzSchemaName, N']') + '.' + QUOTENAME(@BlitzTableName, N']') + '
        WHERE
            checkid = 92  
            AND CheckDate > getdate() - 30
            AND servername = ''' + @TargetInstanceName + '''
            AND Finding like ''Drive %'' + ''' + @TempFilesDrive + ''' + ''% Space'';'        
            
    SELECT 
        @paramlist = '@xMinMBFree NUMERIC(10,1) OUTPUT,
            @xAvgMBFree NUMERIC(10,1) OUTPUT,
            @xMaxMBFree NUMERIC(10,1) OUTPUT'; 
                                                                                                
    EXEC sp_executesql @sql, @paramlist, 
        @MinMBFree OUTPUT, @AvgMBFree OUTPUT, @MaxMBFree OUTPUT;     
    
    PRINT 'MIN MB Free this month: ' + CAST(@MinMBFree AS NVARCHAR(20)) + ' <-- Use this';
    PRINT 'AVG MB Free this month: ' + CAST(@AvgMBFree AS NVARCHAR(20));
    PRINT 'MAX MB Free this month: ' + CAST(@MaxMBFree AS NVARCHAR(20));
    -- todo: check that valid data coming from here first then if not ... SET @VerifyCode = CASE WHEN @InstanceCountTempFiles <> @LastServerwideCountTempFiles THEN @VerifyCode + 330000 ELSE @VerifyCode END
    

    -- [4] -- get the filenames for the ALTER TABLE commands
    CREATE TABLE #TargetTempdbFiles
        (
        LogicalFileName NVARCHAR(40)
        );

    SELECT @sql =                                                                               
        'SELECT 
            CAST(name AS VARCHAR(8)) as name
        FROM ' + QUOTENAME(@TargetLinkName, N']') + '.tempdb.sys.database_files
        WHERE type = 0 AND name LIKE ''temp%'';'
        
    -- PRINT @sql; -- debug        
                                                                                                  
    INSERT INTO #TargetTempdbFiles EXEC sp_executesql @sql;

    INSERT @TempdbFiles (LogicalFileName)
    SELECT LogicalFileName from #TargetTempdbFiles

    SELECT LogicalFileName
    FROM @TempdbFiles;

    -- [5] -- calculate file MAXSIZE
    IF @LastServerwideCountTempFiles = 0
    BEGIN 
        SET @errormessage = N'error, invalid value for @LastServerwideCountTempFiles=' +isnull(''''+  @LastServerwideCountTempFiles  +'''','null');
        SET @errornumber = 99999;
        THROW 99999, @errormessage, 1; 
    END
    SET @MaxAvailablePerTempFile = (@MinMBFree - @MBToReserve)/@LastServerwideCountTempFiles;
    PRINT '@MaxAvailablePerTempFile: ' + CAST(@MaxAvailablePerTempFile AS NVARCHAR(20));
    
    SET @RecommendedNewSize = @LargestTempfileSizeInMB + ROUND(ISNULL(@MaxAvailablePerTempFile/1024, 0), 0)*1024;
    PRINT '@RecommendedNewSize: ' + CAST(@RecommendedNewSize AS NVARCHAR(20));

    SET @RecommendedNewSizeRounded = ROUND(ISNULL(@RecommendedNewSize/1024, 0), 0)*1024;
    PRINT '@RecommendedNewSizeRounded: ' + CAST(@RecommendedNewSizeRounded AS NVARCHAR(20));
    
    PRINT 'Here are the recommendations:'
    SELECT 
        'alter database [tempdb] modify file (NAME = N''' + LogicalFileName + ''', MAXSIZE = ' + CAST(@RecommendedNewSizeRounded AS NVARCHAR(20)) + ')'
    FROM @TempdbFiles;

    IF (@Action = N'ALTER')
    BEGIN
        select 'todo'
        PRINT 'altering tempdb on ' + @TargetInstanceName  
        --SELECT @AlterSQL =
        --    'alter database [tempdb] modify file (NAME = N''' + LogicalFileName + ''', MAXSIZE = ' + CAST(@RecommendedNewSizeRounded AS NVARCHAR(20)) + ')'
        --FROM @TempdbFiles;

    END
    
END


ELSE -- user entered an invalid @Action value
BEGIN

    -- throw error since @Action is incorrect
    SET @errormessage = N'error, invalid value for @Action=' +isnull(''''+  @Action  +'''','null')
    SET @errornumber = 99999
    ;THROW 99999, @errormessage, 1; 
END


END
GO


