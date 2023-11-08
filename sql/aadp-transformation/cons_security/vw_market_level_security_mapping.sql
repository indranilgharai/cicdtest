/****** Object:  View [cons_security].[vw_market_level_security_mapping]    Script Date: 1/08/2023 11:05:08 AM ******/
/****** Modified Object:  View [cons_security].[vw_market_level_security_mapping] Modified Date: 8/08/2023 11:05:08 AM Modified by: Anthony Ladson ******/
/****** Modified Object:  View [cons_security].[vw_market_level_security_mapping] Modified Date: 10/12/2023 03:05:08 PM Modified by: Anthony Ladson ******/

SET
  ANSI_NULLS ON
GO
SET
  QUOTED_IDENTIFIER ON
GO
  CREATE VIEW [cons_security].[vw_market_level_security_mapping] AS 
  with access_level as(
    select
      distinct [BusinessTitle],
      case
        when [BusinessTitle] like '%Online%'
			or [BusinessTitle] like '%Customer Service%'
			or [BusinessTitle]  in (
										'Global Customer & Store Experience Manager'
									,'Global Retail Operations POSM Development Specialist'
									)
			then 'Global'

        when [BusinessTitle] like '%Store Manager%'
			or [BusinessTitle] like '%Counter Manager%'
			or [BusinessTitle] like '%Consultant%'
			or [BusinessTitle] like ('%Facial Therapist%')
       
        or [BusinessTitle] in (
          'Retail Operations Coordinator, West Coast',
          
          'East Coast Retail Operations Coordinator',
          'Store Operations Coordinator',
          'Online Coordinator',
          'Online Operations Coordinator',
          'Retail Development Senior Coordinator',
          'Asst. Retail Operation Manager',
          'Local Retail Design & Development Manager',
          'Global Retail Operations POSM Development Specialist',
          'Production Commercialisation Coordinator',
          'Commercialisation Coordinator',
          'Retail Operations Intern',
          'Commercialisation Assistant',
          'Retail Project Coordinator',
          'Retail Design & Development Coordinator, Greater China',
          
          
          'Retail & Office Coordinator',
          'Online Retail Coordinator',
          'Retail Marketing Manager',
          'Online Retail Officer',
          'Retail Operations Manager, Malaysia',
          'National Retail Manager',
          'Retail Communication Coordinator',
          
          
          'Retail Insights & Operations Manager, Europe',
          'Retail Support Coordinator',
          'Online Store Coordinator',
          'Global Customer & Store Experience Manager',
          'Facial Treatment Manager',
          'Assistant Customer Service Manager',
          'Digital Customer Service Manager, Europe',
          'Fulfillment Associate',
          'Digital Fulfillment Manager, DE',
          'Fulfilment Manager',
          'Préparatrice de commandes',
          'Fulfillment Associate',
          'Interim Assistant Store Manager',
          'Interim Store Manager',
          'Online Customer Services Coordinator',
          'Project Manager',
          'Roaming Store Manager, Sydney',
          'Dual Store & Online Manager',
          'Dual Manager',
  
          'Dual Counter Manager',
          'Interim Dual Store Manager',
          'Roaming Store Manager',
          'Digital Fulfilment Coordinator',
		  'Customer Service Assistant'
        ) 
		then 'Market'

        when [BusinessTitle] in (
          'Senior Retail Project Manager',
       
          'Regional Commercial Projects Coordinator',
          'Retail Business Manager, North',
   
          'Regional Assistant Retail Property Manager',
          'Retail Business Manager, West',
          'Regional Retail Operations Manager',
          'Retail Stream Project Manager- China Entry Project',
          'Regional Retail Operations and Project Lead',
          'Senior Retail Business Manager',
          'Retail Operations Coordinator, ANZ',
          'Project Manager, Retail & Offices',
          'On-call Retail Business Manager',
          'Regional Retail Operations Manager, ANZ',
          'Retail Business Manager, Melbourne (Secondment)',
          'Retail Training Coordinator_Maternity Leave(2018.01.23~2019.04.22)',
          'Retail Design and Development Coordinator',
          'Retail Account Manager',
          'Retail Architectual Manager',
          'Local Retail Property Manager',
          'Product Manager, Retail',
          'Retail Business Manager, South',
          'Retail Training & Performance Coordinator',
          'Retail Business Manager, SA',
          'Retail Design Project Manager',
          'Regional Retail Design and Development Lead',
          'Retail Manager',
          'Retail Project Manager',
          'Retail Property Manager',
          'Regional Retail Property Manager, Europe',
          'Regional Retail Property Manager',
          'Retail Architectural Manager',
          'Retail Inventory Management Officer',
          'Retail & Treatments Business Manager',
          'Regional Commercial Project Manager',
          'Retail Application Specialist',
          'Retail Projects Manager',
          'Regional Retail Design and Development Manager, Asia',
          'Retail Design & Development Coordinator',
          'Retail Design & Development Administrator, ANZ',
          'Retail sales Planning Manager',
          'Talent Acquisition Partner, ANZ Retail',
          'Regional Retail Design & Development Manager',
          'Retail Business Manager, NZ',
          'Retail Business Manager',
          'Retail Design & Development Architectural Coordinator, ANZ',
          'Retail Applications Specialist, Asia',
          'Talent Acquisition Specialist, Retail',
          'Retail Design Creative Manager',
          'Retail Design & Development Assistant',
          'Retail Marketing Assistant Manager',
          'Junior Marketing Analyst',
          'Wholesale Account Manager – Department Stores and Travel Retail',
          'Retail Support Manager, ANZ',
          'Store Design Project Manager, Asia',
          'Visual Merchandising Coordinator, Store Openings EU',
          'Visual Merchandiser, Hong Kong & Macau',
          'Assistant Retail Operations Manager, Hong Kong & Macau',
          'Local Operations Manager',
          'Retail Operation Assistant Manager',
          'Retail Operations & Inventory Coordinator, ANZ',
          'Retail Operations Coordinator',
          'Retail Operations Coordinator, Northern Europe',
          'Retail Operations Coordinator, Southern Europe',
          'Customer Experience and Retail Operations Manager, Asia',
          'Retail Operations Supervisor',
          'Retail Operations Manager',
          'Senior Retail Operations Coordinator',
          'Store Sales Manager',
          'Retail Property Manager',
          'Senior Project Manager',
          'Sr. Retail Operation Associate',
          'Sr. Retail Business Manager',
          'Retail Operations Coordinator, Europe',
          'Retail Operations Coordinator, ANZ',
          'Retail Operations Coordinator, Southern Europe',
          'Retail Operation Coordinator',
          'Sr. Retail Business Manager',
          'Interim Retail Business Manager',
          'Duty Free Business Manager',
		  'Retail Operations Coordinator, Hong Kong & Macau',
		  'Retail Project Coordinator, Europe',
          'Retail Operations Manager, Europe',
		  'Retail Design & Development Coordinator, ANZ',
		  'Retail Design & Development Manager',
		  'Retail Operations Assistant'
        ) 
		or [BusinessTitle] like '%Cluster Manager%'

		then 'Regional'
      --  when [BusinessTitle] like '%Consultant%'
     --   or [BusinessTitle] like ('%Facial Therapist%')
     --   or [BusinessTitle] in (
     --     'Customer Service Assistant',
     --   ) then 'homeStore'
        when b.store_name is null
        or a.location in (
          '88 Langridge Street',
          '120 Oxford St Melbourne'
        ) then 'Global'
        else 'Unknown'
      end as accessLevel
    FROM
      [cons_reference].[dim_consultant_view] a
      left join cons_reference.dim_location_view b on a.[homeStore_location_code] = b.locationkey
    where
      1 = 1
      and (
        [EmploymentEndDate] is null
        or convert(date, [EmploymentEndDate], 103) >= getdate()
      )
      and businessTitle is not null
  ),
  locations as (
    select
      distinct sbs_dp_code_short,
      sbs_name,
      sbs_report_region
    from
      cons_reference.dim_location_view
  )

	select
	  main.employeeEmail,
	  main.homeStore_location_code,
	  main.BusinessTitle,
	  main.EmployeeCountry,
	  reg.sbs_report_region,
	  case
		when al.accessLevel = 'Market' then reg.sbs_name
		else coalesce(locRegion.sbs_name, locGlobal.sbs_name)
	  end as sbs_name,
	  al.accessLevel
	from
	  [cons_reference].[dim_consultant_view] main
	  left join (
		select
		  distinct sbs_dp_code_short,
		  sbs_report_region,
		  sbs_name
		from
		  cons_reference.dim_location_view
	  ) reg on main.employeeCountry = reg.sbs_dp_code_short
	  left join access_level al on main.[BusinessTitle] = al.[BusinessTitle]
	  left join locations locRegion on case
		when al.accessLevel = 'Regional' then reg.sbs_report_region
	  end = locRegion.sbs_report_region
	  left join locations locGlobal on case
		when al.accessLevel = 'Global' then 1
	  end = 1
	where
	  1 = 1
	  and (
		main.[EmploymentEndDate] is null
		or convert(date, main.[EmploymentEndDate], 103) >= getdate()
	  )
	  and main.businessTitle is not null
;
GO