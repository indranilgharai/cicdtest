/****** Object:  View [cons_reference].[dim_date_view]    Script Date: 12/4/2022 9:39:48 AM ******/
SET
	ANSI_NULLS ON
GO
SET
	QUOTED_IDENTIFIER ON
GO
	CREATE VIEW [cons_reference].[dim_date_view] AS
select
	distinct format(incr_date, 'yyyyMMdd') date_key,
	[incr_date] as "date",
	datename(dayofyear, incr_date) day_of_year,
	[day_val] as day_of_month,
	ceiling(cast([day_val] / 7.0 as float)) as week_of_month,
	[day_of_week],
	[day_of_week_string] as day_name,
	DATEADD(d, - day_of_week + 1, incr_date) start_of_week,
	DATEADD(d, - day_of_week + 7, incr_date) end_of_week,
	DATEADD(d, - day_of_week -6, incr_date) start_of_previous_week,
	DATEADD(d, - day_of_week, incr_date) end_of_previous_week,
	DATEADD(d, - day_of_week + 8, incr_date) start_of_next_week,
	DATEADD(d, - day_of_week + 14, incr_date) end_of_next_week,
	upper([business_day]) [business_day],
	[month_id] as month_number,
	[month_name],
	DATEADD(DAY, 1, EOMONTH(incr_date, -1)) start_of_month,
	EOMONTH(incr_date) end_of_month,
	DATEADD(DAY, 1, EOMONTH(incr_date, -2)) start_of_previous_month,
	EOMONTH(incr_date, -1) end_of_previous_month,
	DATEADD(DAY, 1, EOMONTH(incr_date)) start_of_next_month,
	EOMONTH(incr_date, 1) end_of_next_month,
	DAY(EOMONTH(incr_date)) days_in_month,
	[quarter_no] as quarter_number,
	[quarter_name],
	cast(DATEADD(q, DATEDIFF(q, 0, incr_date), 0) as date) start_of_quarter,
	cast(
		DATEADD(
			d,
			-1,
			DATEADD(q, DATEDIFF(q, 0, incr_date) + 1, 0)
		) as date
	) end_of_quarter,
	cast(
		DATEADD(q, DATEDIFF(q, 0, incr_date) -1, 0) as date
	) start_of_previous_quarter,
	cast(
		DATEADD(d, -1, DATEADD(q, DATEDIFF(q, 0, incr_date), 0)) as date
	) end_of_previous_quarter,
	cast(
		DATEADD(q, DATEDIFF(q, 0, incr_date) + 1, 0) as date
	) start_of_next_quarter,
	cast(
		DATEADD(
			d,
			-1,
			DATEADD(q, DATEDIFF(q, 0, incr_date) + 2, 0)
		) as date
	) end_of_next_quarter,
	[yearval] as "year",
	concat([quarter_name], ' ', [yearval]) full_quarter_name,
	[week_of_year],
	md_record_written_timestamp,
	md_record_written_pipeline_id,
	md_transformation_job_id,
	md_source_system
from
	std.date_dim;

GO