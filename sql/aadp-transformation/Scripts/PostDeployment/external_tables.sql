DECLARE @is_error BIT;
DECLARE @error_seed_tbl NVARCHAR(MAX)
DECLARE @error_message NVARCHAR(MAX)
DECLARE @error_number NVARCHAR(MAX)
DECLARE @error_state NVARCHAR(MAX)
BEGIN TRY
	BEGIN
	    print '******Seeding transform_job_control table******';
		SET @error_seed_tbl = 'JOB CONTROL'
	    exec meta_ctl.sp_transform_load_job_control;
	END
	BEGIN
	    print '******Seeding transform_job_master table******';
		SET @error_seed_tbl = 'JOB LOAD MASTER'
	    exec meta_ctl.sp_transform_load_job_master;
	END
	BEGIN
	    print '******Seeding transform_job_steps table******';
		SET @error_seed_tbl = 'JOB LOAD STEPS'
	    exec meta_ctl.sp_transform_load_job_steps;
	END
	
	print 'Data seed Successful !! Cleaning up external resources created'
    SET @is_error=1
END TRY
BEGIN CATCH
    print 'Data seed failed !! Cleaning up external resources created'
    SET @is_error=0
    SET @error_message=CONCAT(@error_seed_tbl,' ::: ',ERROR_MESSAGE())
    SET @error_number=50005
    SET @error_state=ERROR_STATE()
END CATCH

BEGIN
    exec meta_ctl.sp_transform_post_deploy_cleanup_handler;
    IF @is_error=0
        THROW @error_number,@error_message,@error_state;
END
