param
(
    [parameter(Mandatory = $true)] [String] $resourceGroupName,
    [parameter(Mandatory = $true)] [String] $storageAccountName,
    [parameter(Mandatory = $false)] [Bool] $predeployment=$true,
    [parameter(Mandatory = $false)] [String] $ip
)



# #Connects to Azure
# Connect-AzAccount

#Grabs the Public IP of the runner
$publicip = (Invoke-WebRequest -uri "https://api.ipify.org/").Content


Write-Host "$publicip"

if ($predeployment -eq $true) {
    Add-AzStorageAccountNetworkRule -ResourceGroupName $resourceGroupName -AccountName $storageAccountName -IPAddressOrRange "$publicip"
}
else{
    Remove-AzStorageAccountNetworkRule -ResourceGroupName $resourceGroupName -AccountName $storageAccountName -IPAddressOrRange "$publicip"
}

return $publicip
