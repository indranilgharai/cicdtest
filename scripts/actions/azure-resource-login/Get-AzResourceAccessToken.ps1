# param
# (
#     [Parameter(Mandatory)]
#     [String]$ResourceUrl,
#     [String]$ResourceTypeName= "Arm"
# )

# Set-PSRepository PSGallery -InstallationPolicy Trusted
# Install-Module -Name Az.Accounts -AllowClobber

# $context = Get-AzContext
# $resourceToken = (Get-AzAccessToken -ResourceUrl $ResourceUrl -DefaultProfile $context).Token

# c

param
(
    [Parameter(Mandatory)]
    [String]$ClientID,
    [String]$ClientSecret,
    [String]$ResourceUrl,
    [String]$ResourceTypeName= "Arm"
)

Set-PSRepository PSGallery -InstallationPolicy Trusted
Install-Module -Name Az.Accounts -AllowClobber

# $context = Get-AzContext
$tenantID = Get-AzTenant | Select-Object -ExpandProperty Id

$Fields = @{
  grant_type    = "client_credentials"
  client_id     = $ClientID
  resource      = $ResourceUrl
  client_secret = $ClientSecret
};

$response = Invoke-RestMethod `
    –Uri "https://login.microsoftonline.com/$tenantID/oauth2/token" `
    –ContentType "application/x-www-form-urlencoded" `
    –Method POST `
    –Body $Fields;

$resourceToken =  $response.access_token
return $resourceToken