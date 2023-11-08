azure_storage_account=$1
resource_group_name=$2
environment=$3
storage_file_system=staging

azure_storage_key=$(az storage account keys list \
    --account-name "$azure_storage_account" \
    --resource-group "$resource_group_name" \
    --output json |
    jq -r '.[0].value')


echo "Uploading seed data to /staging/data/transformation/seed/"

echo "Uploading transform_job_control"
az storage blob upload --container-name $storage_file_system --account-name "$azure_storage_account" --account-key "$azure_storage_key" \
    --file sql/data/${environment}/transform_job_control.csv --name "data/transformation/seed/transform_job_control/transform_job_control.csv" \
    --overwrite true

echo "Uploading transform_job_master"
az storage blob upload --container-name $storage_file_system --account-name "$azure_storage_account" --account-key "$azure_storage_key" \
    --file sql/data/${environment}/transform_job_master.csv --name "data/transformation/seed/transform_job_master/transform_job_master.csv" \
    --overwrite true

echo "Uploading transform_job_steps"
az storage blob upload --container-name $storage_file_system --account-name "$azure_storage_account" --account-key "$azure_storage_key" \
    --file sql/data/${environment}/transform_job_steps.csv --name "data/transformation/seed/transform_job_steps/transform_job_steps.csv" \
    --overwrite true