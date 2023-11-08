/****** Object: [cons_reference].[dwh_digital_location_view] 
Purpose: DAT 1025 Create consumption view of stage.dwh_digital_location_view 
Created Date: 10/12/2023 4:55:10 AM  Created By: Abhinav Tiwari ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [cons_reference].[dwh_digital_location_view]
AS select
*
from
stage.dwh_digital_locations;
GO
