// See https://aka.ms/new-console-template for more information
using Parquet.Data.Rows;
using Parquet.Data;
using Parquet;
using System.Data.Common;

Console.WriteLine("Hello, Parquet!");

// e.g. Metadata

var metaDataTableSchema = new Schema(
      new DataField<int>("Id"),
      new DataField<double>("DoubleField"),
      new DataField<string>("StringField"));
var metaDataTable = new Table(metaDataTableSchema);



var random = new Random();



for (int i = 0; i < 3; i++)
{
    metaDataTable.Add(new Row(i, random.NextDouble(), $"StringContent {i}"));
    Console.WriteLine($"Added meta data: {metaDataTable[i]}");

    var detailDataTable = new Table(
    new Schema(
      new DataField<int>("Id"),
      new DataField<double>("DoubleField"),
      new DataField<string>("StringField"),
      new DataField<int>("MetaDataId")));

    
    for (int j = 0; j < 1000000; j++)
    {
        detailDataTable.Add(new Row(j, random.NextDouble(), $"StringContent {j}", i));
        Console.WriteLine($"Added detail data: {detailDataTable[j]}");
    }

    using (Stream fileStream = System.IO.File.OpenWrite($"detail_data_{i}.parquet.gzip"))
    {
        using var parquetWriter = await ParquetWriter.CreateAsync(detailDataTable.Schema, fileStream);
        parquetWriter.CompressionMethod = CompressionMethod.Gzip;
        await parquetWriter.WriteAsync(detailDataTable);
    }
}

using (Stream fileStream = System.IO.File.OpenWrite("meta_data_1.parquet.gzip"))
{
    using var parquetWriter = await ParquetWriter.CreateAsync(metaDataTableSchema, fileStream);
    parquetWriter.CompressionMethod = CompressionMethod.Gzip;
    await parquetWriter.WriteAsync(metaDataTable);
}





