/****** Object:  Table [stage].[itemcost_excluded_stock_list]    Script Date: 12/16/2022 4:14:35 AM ******/
SET ANSI_NULLS ON
GO

 

SET QUOTED_IDENTIFIER ON
GO

 

CREATE TABLE [stage].[itemcost_excluded_stock_list]
(
    [Store_no] [nvarchar](max) NULL,
    [Store] [nvarchar](max) NULL,
    [SKU] [nvarchar](max) NULL,
    [Description] [nvarchar](max) NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
)
GO