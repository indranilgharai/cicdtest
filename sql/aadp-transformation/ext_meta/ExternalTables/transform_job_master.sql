CREATE EXTERNAL TABLE [ext_meta].[transform_job_master] (
    [job_id] [int] NULL,
	[job_name] [varchar](400) NULL,
	[description] [varchar](4000) NULL,
	[usecase_tag] [varchar](400) NULL
) WITH (
    data_source = [ADLSTransformation],
    location = N'data/transformation/seed/transform_job_master/',
    file_format = [CSVSkipHeaderFileFormat],
    reject_type = VALUE,
    reject_value = 0
);