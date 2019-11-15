USE master
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_maxsize_tempdb]
    @Action                         NVARCHAR(64)    = NULL,
    @LastServerwideCountTempFiles   INT             = NULL,
    @LastTempFilesDrive             NCHAR(1)        = NULL,
    @MBToReserve                    NUMERIC(10,1)   = NULL,
    @BlitzDatabaseName              NVARCHAR(256)   = NULL,
    @BlitzSchemaName                NVARCHAR(256)   = NULL,
    @BlitzTableName                 NVARCHAR(256)   = NULL,
    @TargetInstanceName             NVARCHAR(256)   = NULL,
    @TargetLinkName                 NVARCHAR(256)   = NULL,
    @Verbose                        NCHAR(1)        = NULL
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
--      EXEC sp_maxsize_tempdb @Action = N'VERIFY', @LastServerwideCountTempFiles = 8, @LastTempFilesDrive = 'E', @MBToReserve = 5120, @BlitzDatabaseName = N'dbadatabase', @BlitzSchemaName = N'dbo', @BlitzTableName = N'BlitzResults', @TargetInstanceName = N'testdbserver', @TargetLinkName = N'testdbserver_linkname', @Verbose = N'Y';
--      EXEC sp_maxsize_tempdb @Action = N'RECOMMEND', @LastServerwideCountTempFiles = 8, @LastTempFilesDrive = 'E', @MBToReserve = 5120, @BlitzDatabaseName = N'dbadatabase', @BlitzSchemaName = N'dbo', @BlitzTableName = N'BlitzResults', @TargetInstanceName = N'testdbserver', @TargetLinkName = N'testdbserver_linkname', @Verbose = N'Y';
--      EXEC sp_maxsize_tempdb @Action = N'Typographical Error';
--
-- dependencies:
--      1) a sqlagent job to run this via PowerShell daily
--      2) depends on SQL Server 2016 SP1 or newer for the mgmt server that runs this sproc b/c of CREATE OR ALTER syntax above
--      3) depends on sp_Blitz repository table on a mgmt server for calculating volume statistics
--
-- updated:
--      -- Monday, March 18, 2019 12:33 PM
--      -- Friday, November 15, 2019 3:02 PM
-- 

BEGIN
SET NOCOUNT ON;
DECLARE @errormessage NVARCHAR(2048), @errornumber INT, @errorseverity INT, @errorstate INT, @errorline INT;
DECLARE @LargestTempfileSizeInMB NUMERIC(10,1);
DECLARE @LargestMaxTempfileSizeInMB NUMERIC(10,1);
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
DECLARE @RecommendedNewSizeRounded  INT;
DECLARE @RecommendedProgression     NVARCHAR(MAX);
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

    IF (@Verbose = N'Y')
    BEGIN
        PRINT '----------------------------------------------'      
        PRINT '[1] get current max tempdb filesize for [c20]: @LargestTempfileSizeInMB = ' + cast(@LargestTempfileSizeInMB as VARCHAR(20)); 
    END

    -- [1b] -- get current max of maxsize for tempdb files 
    SELECT @sql =                                                                               
        'SELECT 
            @xLargestMaxTempfileSizeInMB = MAX(CAST(maxsize/128.0 AS NUMERIC(10,1))) 
        FROM ' + QUOTENAME(@TargetLinkName, N']') + '.tempdb.dbo.sysfiles
        WHERE groupid = 1' 
    
    SELECT 
        @paramlist = '@xLargestMaxTempfileSizeInMB NUMERIC(10,1) OUTPUT'                                                                   
                                                                                                
    EXEC sp_executesql @sql, @paramlist, 
        @LargestMaxTempfileSizeInMB OUTPUT;

    IF (@Verbose = N'Y')
    BEGIN
        PRINT '----------------------------------------------'      
        PRINT '[1b] get current max of maxsize for tempdb files : @LargestMaxTempfileSizeInMB = ' + cast(@LargestMaxTempfileSizeInMB as VARCHAR(20)); 
    END

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

    IF (@Verbose = N'Y')
    BEGIN
        PRINT '----------------------------------------------'      
        PRINT '[2a] -- get current drivename for [c4] and test against where we think the temp file should be:  @TempFilesDrive = ' + @TempFilesDrive;
        PRINT 'Are tempfiles are still on the expected drive?: ' + CASE WHEN @TempFilesDrive <> @LastTempFilesDrive THEN 'false (Warning! The drive that we expect to hold tempdb has changed)' ELSE 'true' END;
    END


    
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

    IF (@Verbose = N'Y')
    BEGIN
        PRINT '----------------------------------------------'      
        PRINT '[2b] to calculate [c13] take the count of nTempFiles, Compare to last known number of temp files, or rerun on ALL instances:  @InstanceCountTempFiles = ' + cast(@InstanceCountTempFiles AS NVARCHAR(20));
        PRINT 'Looking at this parameter... on a multi-instance server we are sending in @LastServerwideCountTempFiles as our expected server-wide count of temp files'; 
        PRINT 'Are all serverwide tempfiles on this drive only for this instance?: ' + CASE WHEN @InstanceCountTempFiles <> @LastServerwideCountTempFiles THEN 'false (Warning! We need to count the tempfiles on all instances.)' ELSE 'true' END;
    END

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
    
    IF (@Verbose = N'Y')
    BEGIN
        PRINT '----------------------------------------------'      
        PRINT '[3] -- get latest montly trend on free space from sp_Blitz() available on the drive [c4] and put the low point into [c6]';
        PRINT 'MIN MB Free this month: ' + CAST(@MinMBFree AS NVARCHAR(20)) + ' <-- Use this';
        PRINT 'AVG MB Free this month: ' + CAST(@AvgMBFree AS NVARCHAR(20));
        PRINT 'MAX MB Free this month: ' + CAST(@MaxMBFree AS NVARCHAR(20));
    END
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
        
    INSERT INTO #TargetTempdbFiles EXEC sp_executesql @sql;

    INSERT @TempdbFiles (LogicalFileName)
    SELECT LogicalFileName from #TargetTempdbFiles

    IF (@Verbose = N'Y')
    BEGIN
        PRINT '----------------------------------------------'      
        PRINT '[4] -- get the filenames for the ALTER TABLE commands';
        PRINT @sql;         
        SELECT LogicalFileName from #TargetTempdbFiles
    END
                                                                                                  

    -- [5] -- calculate file MAXSIZE
    IF @LastServerwideCountTempFiles = 0
    BEGIN 
        SET @errormessage = N'error, invalid value for @LastServerwideCountTempFiles=' +isnull(''''+  @LastServerwideCountTempFiles  +'''','null');
        SET @errornumber = 99999;
        THROW 99999, @errormessage, 1; 
    END
    SET @MaxAvailablePerTempFile = (@MinMBFree - @MBToReserve)/@LastServerwideCountTempFiles;   
    SET @RecommendedNewSize = @LargestTempfileSizeInMB + FLOOR(ISNULL(@MaxAvailablePerTempFile/1024, 0))*1024;
    SET @RecommendedNewSizeRounded = CAST(FLOOR(ISNULL(@RecommendedNewSize/1024, 0))*1024 AS INT);

    IF (@Verbose = N'Y')
    BEGIN
        PRINT '----------------------------------------------'      
        PRINT '[5] -- calculate file MAXSIZE';
        PRINT '@MaxAvailablePerTempFile: ' + CAST(@MaxAvailablePerTempFile AS NVARCHAR(20));
        PRINT '@RecommendedNewSize: ' + CAST(@RecommendedNewSize AS NVARCHAR(20));
        PRINT '@RecommendedNewSizeRounded: ' + CAST(@RecommendedNewSizeRounded AS NVARCHAR(20));
    END
    
    -- This section (current settings) is presented regardless of @Verbose flag
    PRINT '----------------------------------------------'      
    PRINT 'Here are the current settings for ' + @TargetInstanceName  + ':'

    SELECT @sql =                                                                               
        'SELECT LEFT(name, 20) AS name, CASE WHEN maxsize = -1 THEN 2097152 ELSE (maxsize/128) END  as current_file_maxsize_mb
        FROM ' + QUOTENAME(@TargetLinkName, N']') + '.tempdb.dbo.sysfiles 
        WHERE groupid = 1
        ORDER BY fileid'

    EXEC sp_executesql @sql;
    
    -- for recommendation text .. figure out to say whether recommendations are 'decreasing', 'same' or 'increasing'
    SET @RecommendedProgression = CASE 
        WHEN (@LargestMaxTempfileSizeInMB < @RecommendedNewSizeRounded) THEN 'increase - more space avail for TempDB'
        WHEN (@LargestMaxTempfileSizeInMB > @RecommendedNewSizeRounded) THEN 'decrease - less space avail for TempDB'        
        ELSE 'same' END;

    -- for recommendation text .. redesign the 99999 exception, treat it for what it really means - inability to maintain the goal minimum disk space on the drive.
    SET @RecommendedProgression = @RecommendedProgression + CASE 
        WHEN (@RecommendedNewSizeRounded < @LargestTempfileSizeInMB) THEN ' - WARNING!  We have reached a failure to maintain the goal minimum disk space on the drive.  What this means for the dba is that either the disk needs to be enlarged at the operating system level, or utilization of tempdb needs to change because at this point an application flooding tempdb will soon be generating error Msg 1101.'
        ELSE '' END;
    
    -- This section (recommendation) is presented regardless of @Verbose flag
    PRINT '----------------------------------------------'      
    PRINT 'Here are the recommendations for ' + @TargetInstanceName  + ':    ' + @RecommendedProgression;
    SELECT 
        'ALTER DATABASE [tempdb] MODIFY FILE (NAME = N''' + LogicalFileName + ''', MAXSIZE = ' + CAST(@RecommendedNewSizeRounded AS NVARCHAR(20)) + ');' + CHAR(13) + CHAR(10) + N'GO' + CHAR(13) + CHAR(10)
    FROM @TempdbFiles;
    
    IF (@Action = N'ALTER')
    BEGIN
        DECLARE @altersqlline NVARCHAR(1024) = ''; 
        DECLARE @altersqlfile NVARCHAR(1024) = ''; 
         
        DECLARE c CURSOR LOCAL FAST_FORWARD FOR
            SELECT N'ALTER DATABASE [tempdb] MODIFY FILE (NAME = N''' + LogicalFileName + N''', MAXSIZE = '' + CAST(@xRecommendedNewSizeRounded AS NVARCHAR(20)) + '');''' + CHAR(13) + CHAR(10) + N'GO' + CHAR(13) + CHAR(10) AS altersql FROM #TargetTempdbFiles
        
        OPEN c;
        FETCH c INTO @altersqlline;
        
        WHILE (@@FETCH_STATUS = 0)
        BEGIN
            SET @altersqlfile = @altersqlfile + @altersqlline;
            FETCH c INTO @altersqlline;
        END
         
        CLOSE c;
        DEALLOCATE c;
         
        IF (@Verbose = N'Y')
        BEGIN
            PRINT '----------------------------------------------'      
            PRINT 'Here is the parameterized SQL batch for the ALTER statements: ' 
            PRINT @altersqlfile;         
        END    
       
        --SELECT @sql = @altersqlfile;                                                                              
        --    
        --SELECT @paramlist = '@xRecommendedNewSizeRounded NUMERIC(10,1) OUTPUT'; 
        --
        --EXEC [RemoteServer].master.dbo.sp_executesql @sql, @paramlist, 
        --    @RecommendedNewSizeRounded OUTPUT;     
    
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


