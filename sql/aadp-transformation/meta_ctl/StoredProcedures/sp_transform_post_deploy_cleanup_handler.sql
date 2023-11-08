SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [meta_ctl].[sp_transform_post_deploy_cleanup_handler]
AS
BEGIN
    PRINT '******Dropping External resources******';

    DECLARE @sql AS NVARCHAR(MAX);

    WITH
        cmds
        AS
        (
            SELECT CONCAT (
			'drop external table ',
			quotename(object_schema_name(t.object_id)),
			'.',
			quotename(object_name(t.object_id))
			) AS cmd
                FROM sys.external_tables t
                where t.name like 'transform_%'

            UNION ALL

                SELECT 'drop external data source ' + quotename(s.name)
                FROM sys.external_data_sources s
                where s.name = 'ADLSTransformation'
        )
    SELECT @sql = string_agg(cmd, ';')
    FROM cmds

    PRINT @sql

    IF (len(@sql) > 0)
    BEGIN
        EXEC sp_executesql @sql
    END

END