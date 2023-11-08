CREATE PROC [meta_ctl].[sp_transform_load_job_steps] AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
/*
    DECLARE @duser VARCHAR(50)=USER_NAME()
    DECLARE @ddate DATETIME =GETUTCDATE()
*/
    TRUNCATE TABLE meta_ctl.[transform_job_steps];
	INSERT INTO meta_ctl.[transform_job_steps]
	SELECT
		[job_id],
		[step_number],
		[sp_name],
		[sp_parms],
		[dependent_step_num]
	FROM [ext_meta].[transform_job_steps];

END