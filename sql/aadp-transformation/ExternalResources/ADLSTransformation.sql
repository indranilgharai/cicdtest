CREATE EXTERNAL DATA SOURCE [ADLSTransformation]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'abfss://staging@$(STORAGE_ACCOUNT).dfs.core.windows.net',
    CREDENTIAL = [msi_cred]
    );