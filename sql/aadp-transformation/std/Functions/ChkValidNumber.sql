SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [std].[ChkValidNumber] (@storx_sbs_no [varchar](10)) RETURNS bit
AS
BEGIN     
  DECLARE @bitNumVal as Bit
  DECLARE @NumText varchar(100)

  SET @NumText=ltrim(rtrim(isnull(@storx_sbs_no,'')))

  SET @bitNumVal = case when PATINDEX('%[0-9]%', @NumText)=1 then 1
                          else 0 
                      end
  RETURN @bitNumVal
END