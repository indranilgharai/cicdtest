CREATE EXTERNAL TABLE [ext_meta].[transform_job_steps] (
    [job_id] [int] NULL,
	[step_number] [int] NULL,
	[sp_name] [varchar](400) NULL,
	[sp_parms] [varchar](400) NULL,
	[dependent_step_num] [varchar](400) NULL
) WITH (
    data_source = [ADLSTransformation],
    location = N'data/transformation/seed/transform_job_steps/',
    file_format = [CSVSkipHeaderFileFormat],
    reject_type = VALUE,
    reject_value = 0
);