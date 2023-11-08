/****** Object:  View [cons_reference].[dim_exchangeRate_view]    Script Date: 12/5/2022 8:47:41 AM ******/
SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE VIEW [cons_reference].[dim_exchangeRate_view] AS
select
    distinct cast(ex.sbs_no as varchar(10)) ExchangeRateKey,
    cast(sub.sbs_currency_code as nvarchar(100)) as Currency_Code,
    cast(sub.sbs_currency_name as nvarchar(100)) as Currency_Name,
    cast(sub.sbs_currency_symbol as nvarchar(100)) as Currency_Symbol,
    cast(ex.ex_rate as float) as Current_FX_Rate_From_AUD
    
from
    (
        SELECT
            *
        FROM
            (
                SELECT
                    sbs_no,
                    ex_rate,
                    year,
                    month_no,
                    ROW_NUMBER() OVER(
                        PARTITION BY sbs_no
                        ORDER BY
                            year DESC,
                            month_no DESC
                    ) rwno
                FROM
                    [std].[exchange_rate_x]
            ) a
        WHERE
            rwno = 1
    ) ex 
    join std.subsidiary_x sub on sub.sbs_no = ex.sbs_no;

GO