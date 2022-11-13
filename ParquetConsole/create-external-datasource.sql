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