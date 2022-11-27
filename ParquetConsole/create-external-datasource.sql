CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<password>' ;

CREATE DATABASE SCOPED CREDENTIAL AccessAzureInvoices
WITH
  IDENTITY = 'SHARED ACCESS SIGNATURE',
  -- Remove ? from the beginning of the SAS token
  SECRET = '<azure_shared_access_signature>' ;

CREATE EXTERNAL DATA SOURCE ParquetStorage
WITH
  ( LOCATION = 'adls://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/<FOLDER>' ,
    CREDENTIAL = AzureStorageCredential ,
  ) ;

CREATE EXTERNAL FILE FORMAT ParquetFileFormat_compressed
WITH (
         FORMAT_TYPE = PARQUET,
		  , DATA_COMPRESSION = 'org.apache.hadoop.io.compress.GzipCodec'
);

CREATE EXTERNAL TABLE [dbo].[DetailData] (  
      [Id] int,
      [DoubleField] float,
      [StringField] NVARCHAR(MAX),
	  [MetaDataId] int
)  
WITH (LOCATION='/<SUB_FOLDER>/',
      DATA_SOURCE = ParquetStorage,  
      FILE_FORMAT = ParquetFileFormat_compressed  
);

CREATE EXTERNAL TABLE [dbo].[MetaData] (  
      [Id] int,
      [DoubleField] float,
      [StringField] NVARCHAR(MAX),
	  [NonExistingField] int
)  
WITH (LOCATION='/<OTHER_SUB_FOLDER>/',
      DATA_SOURCE = ParquetStorage,  
      FILE_FORMAT = ParquetFileFormat_compressed  
);

/* Example query */
SELECT 
	DD.Id,
	DD.DoubleField,
	MD.Id,
	MD.StringField
FROM [dbo].[MetaData] MD
INNER JOIN [dbo].[DetailData] DD
	ON MD.Id = DD.MetaDataId
		AND MD.Id = 2

-- Compression specification is not necessary
SELECT TOP 100 *
FROM OPENROWSET(BULK '/metadata/',
                DATA_SOURCE= 'ParquetStorage', --> Root URL is in LOCATION of DATA SOURCE
                FORMAT= 'PARQUET'
				) AS [file]
