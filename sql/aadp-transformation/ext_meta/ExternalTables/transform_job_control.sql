CREATE EXTERNAL TABLE [ext_meta].[transform_job_control] (
	[ZoneName] [varchar](20) NOT NULL,
	[BatchID] [int] NOT NULL,
	[JobID] [int] NOT NULL,
	[SourceType] [varchar](50) NULL,
	[SourceFormat] [varchar](100) NULL,
	[SourceFormatSubType] [varchar](100) NULL,
	[EnabledFlag] [bit] NOT NULL,
	[snapshot_flag] [int] NOT NULL,
	[dependent_job] [varchar](1000) NULL,
	[stage_number] [tinyint] NULL,
	[reset_flag] [bit] NOT NULL
) WITH (
    data_source = [ADLSTransformation],
    location = N'data/transformation/seed/transform_job_control/',
    file_format = [CSVSkipHeaderFileFormat],
    reject_type = VALUE,
    reject_value = 0
);