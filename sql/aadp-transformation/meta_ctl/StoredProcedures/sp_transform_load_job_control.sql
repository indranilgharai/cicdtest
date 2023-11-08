CREATE PROC [meta_ctl].[sp_transform_load_job_control] AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    DECLARE @duser VARCHAR(50)=USER_NAME()
    DECLARE @ddate DATETIME =GETUTCDATE()

    TRUNCATE TABLE meta_ctl.[transform_job_control];
	INSERT INTO meta_ctl.[transform_job_control]
	SELECT
        [ZoneName]
        ,[BatchID]
        ,[JobID]
        ,[SourceType]
        ,[SourceFormat]
        ,[SourceFormatSubType]
        ,NULL AS SourceStorageAccount
        ,NULL AS SourceName
        ,NULL AS TargetFormat
        ,NULL AS TargetStorageAccount
        ,NULL AS TargetName
        ,[EnabledFlag]
        ,NULL AS Frequency
        ,@ddate
        ,@duser
        ,@ddate
        ,@duser
        ,NULL AS ArchivalLocation
        ,NULL AS metadatafilename
        ,NULL AS TargetPartitionFlag
        ,NULL AS SourceStorageType
        ,[snapshot_flag]
        ,[dependent_job]
        ,[stage_number]
        ,[reset_flag]
	FROM [ext_meta].[transform_job_control];
END
