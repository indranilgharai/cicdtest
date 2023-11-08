CREATE PROC [meta_ctl].[sp_transform_load_job_master] AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
/*
    DECLARE @duser VARCHAR(50)=USER_NAME()
    DECLARE @ddate DATETIME =GETUTCDATE()
*/
    TRUNCATE TABLE meta_ctl.[transform_job_master];
	INSERT INTO meta_ctl.[transform_job_master]
	SELECT
		[job_id],
		[job_name],
		[description],
		[usecase_tag]
	FROM [ext_meta].[transform_job_master];

END