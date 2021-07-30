using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

/// <summary>
/// Sample for performing Streaming Ingestion using Kusto Client Library
/// IMPORTANT NOTE: Streaming Ingestion is in alpha phase and only enabled on specific clusters.
/// This code will not work with most production/lab/ppe/dev clusters.
/// </summary>
/// <remarks>
/// This sample assumes that the cluster has database name StreamTest0 with table EventLog
/// The table has the following schema:
/// .create table EventLog (Timestamp:datetime, EventId:long, EventText:string, Properties:dynamic)
/// The code will create ingestion JSON mapping on the table (if one does not exist already)
/// .create table EventLog ingestion json mapping 'TestJsonMapping'  '[{"column":"Timestamp","path":"$.EventTime","transform":0},{"column":"EventId","path":"$.EventId","transform":0},{"column":"EventText","path":"$.EventText","transform":0},{"column":"Properties","path":"$.Properties","transform":0}]'
/// </remarks>
namespace StreamingIngestionSample
{
    class Program
    {
        private const string s_jsonMappingName = "TestJsonMapping";
        private static readonly ColumnMapping[] s_jsonMapping = new ColumnMapping[]
        {
            new ColumnMapping { ColumnName = "Timestamp",  Properties = new Dictionary<string, string>{ { MappingConsts.Path, "$.EventTime" },  { MappingConsts.TransformationMethod, CsvFromJsonStream_TransformationMethod.None.FastToString() } } },
            new ColumnMapping { ColumnName = "EventId",    Properties = new Dictionary<string, string>{ { MappingConsts.Path, "$.EventId" },    { MappingConsts.TransformationMethod, CsvFromJsonStream_TransformationMethod.None.FastToString() } } },
            new ColumnMapping { ColumnName = "EventText",  Properties = new Dictionary<string, string>{ { MappingConsts.Path, "$.EventText" },  { MappingConsts.TransformationMethod, CsvFromJsonStream_TransformationMethod.None.FastToString() } } },
            new ColumnMapping { ColumnName = "Properties", Properties = new Dictionary<string, string>{ { MappingConsts.Path, "$.Properties" }, { MappingConsts.TransformationMethod, CsvFromJsonStream_TransformationMethod.None.FastToString() } } },
        };

        static void Main(string[] args)
        {
            var kcsb = new KustoConnectionStringBuilder();
            kcsb.DataSource = "https://kustolab.kusto.windows.net";
            kcsb.FederatedSecurity = true;

            string databaseName = "StreamingIngestionSample";
            string tableName = "EventLog";

            CreateJsonMappingIfNotExists(kcsb, databaseName, tableName);

            // Do ingestion using Kusto.Data client library 
            using (var siClient = KustoClientFactory.CreateCslStreamIngestClient(kcsb))
            {
                using (var data = CreateSampleEventLogCsvStream(10))
                {
                    siClient.ExecuteStreamIngestAsync(
                        databaseName,
                        tableName,
                        data,
                        null,
                        DataSourceFormat.csv);
                }

                using (var data = CreateSampleEventLogJsonStream(10))
                {
                    siClient.ExecuteStreamIngestAsync(
                        databaseName,
                        tableName,
                        data,
                        null,
                        DataSourceFormat.json,
                        compressStream: false,
                        mappingName: s_jsonMappingName).ResultEx();
                }
            }

            // Do ingestion using Kusto.Ingest client library. The data still goes directly to the engine cluster
            // Just a convenience for applications already using IKustoIngest interface
            using (var ingestClient = KustoIngestFactory.CreateStreamingIngestClient(kcsb))
            {
                using (var data = CreateSampleEventLogCsvStream(10))
                {
                    var ingestProperties = new KustoIngestionProperties(databaseName, tableName)
                    {
                        Format = DataSourceFormat.csv,
                    };
                    ingestClient.IngestFromStreamAsync(data, ingestProperties).ResultEx();
                }

                using (var data = CreateSampleEventLogJsonStream(10))
                {
                    var ingestProperties = new KustoIngestionProperties(databaseName, tableName)
                    {
                        Format = DataSourceFormat.json,
                        IngestionMapping = new IngestionMapping { IngestionMappingKind = Kusto.Data.Ingestion.IngestionMappingKind.Json, IngestionMappingReference = s_jsonMappingName }
                    };

                    ingestClient.IngestFromStream(data, ingestProperties);
                }

            }
        }

        /// <summary>
        /// Check table for existense of JSON mapping and create one if necessary
        /// </summary>
        /// <param name="kcsb">KustoConnectionStringBuilder object configured to connect to the cluster</param>
        /// <param name="databaseName">Name of the database</param>
        /// <param name="tableName">Name of the table</param>
        static void CreateJsonMappingIfNotExists(KustoConnectionStringBuilder kcsb, string databaseName, string tableName)
        {
            using (var adminClient = KustoClientFactory.CreateCslAdminProvider(kcsb))
            {
                var showMappingsCommand = CslCommandGenerator.GenerateTableJsonMappingsShowCommand(tableName);
                var existingMappings = adminClient.ExecuteControlCommand<IngestionMappingShowCommandResult>(databaseName, showMappingsCommand);

                if (existingMappings.FirstOrDefault(m => String.Equals(m.Name, s_jsonMappingName, StringComparison.Ordinal)) != null)
                {
                    return;
                }

                var createMappingCommand = CslCommandGenerator.GenerateTableMappingCreateCommand(Kusto.Data.Ingestion.IngestionMappingKind.Json, tableName, s_jsonMappingName, s_jsonMapping);
                adminClient.ExecuteControlCommand(databaseName, createMappingCommand);
            }
        }
        /// <summary>
        /// Create sample data formatted as CSV
        /// </summary>
        /// <param name="numberOfRecords">Number of records to create</param>
        /// <returns>Stream (positioned at the beginning)</returns>
        /// <remarks>
        /// See main file comment for data schema
        /// </remarks>
        private static Stream CreateSampleEventLogCsvStream(int numberOfRecords)
        {
            var ms = new MemoryStream();
            using (var tw = new StreamWriter(ms, Encoding.UTF8, 4096, true))
            {
                for (int i = 0; i < numberOfRecords; i++)
                {
                    tw.WriteLine("{0},{1},{2},{3}", DateTime.Now, i, "Sample event text", "\"{'Prop1':1, 'Prop2': 'Text'}\"");
                }
            }
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        /// <summary>
        /// Create sample data formatted as JSON
        /// </summary>
        /// <param name="numberOfRecords">Number of records to create</param>
        /// <returns>Stream (positioned at the beginning)</returns>
        /// <remarks>
        /// See main file comment for schema in JSON mapping
        /// </remarks>
        private static Stream CreateSampleEventLogJsonStream(int numberOfRecords)
        {
            var ms = new MemoryStream();
            using (var tw = new StreamWriter(ms, Encoding.UTF8, 4096, true))
            {
                for (int i = 0; i < numberOfRecords; i++)
                {
                    tw.WriteLine("{{'EventTime':'{0}','EventId':{1},'EventText':'{2}','Properties':{3}}}", DateTime.Now, i, "Sample event text", "{'Prop1':1, 'Prop2': 'Text'}");
                }
            }
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }
    }
}
