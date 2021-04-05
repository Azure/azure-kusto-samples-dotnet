using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System;
using System.IO;
using System.Linq;
using System.Text;

/// <summary>
/// Sample for performing Streaming Ingestion using Kusto Client Library
/// This code will not work with most production/lab/ppe/dev clusters.
/// Streaming policies must be enabled on the cluster.
/// </summary>
/// <remarks>
/// This sample assumes that the cluster has database named StreamTest0 with table EventLog
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
        private static readonly JsonColumnMapping [] s_jsonMapping = new JsonColumnMapping []
        {
            new JsonColumnMapping { ColumnName = "Timestamp",  JsonPath = "$.EventTime",  TransformationMethod = (TransformationMethod) CsvFromJsonStream_TransformationMethod.None},
            new JsonColumnMapping { ColumnName = "EventId",    JsonPath = "$.EventId",    TransformationMethod = (TransformationMethod) CsvFromJsonStream_TransformationMethod.None},
            new JsonColumnMapping { ColumnName = "EventText",  JsonPath = "$.EventText",  TransformationMethod = (TransformationMethod) CsvFromJsonStream_TransformationMethod.None},
            new JsonColumnMapping { ColumnName = "Properties", JsonPath = "$.Properties", TransformationMethod = (TransformationMethod) CsvFromJsonStream_TransformationMethod.None},
        };

        static void Usage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("\tStreamingIngestionSample -cluster <cluster url> -db <database> -table <table>");
        }

        static bool ParseCommandLine(string[] args, out string cluster, out string database, out string table)
        {
            cluster = null;
            database = null;
            table = null;
            int i = 0;

            while (i < args.Length)
            {
                switch (args[i].ToLowerInvariant())
                {
                    case "-cluster":
                        cluster = args[++i];
                        break;
                    case "-db":
                        database = args[++i];
                        break;
                    case "-table":
                        table = args[++i];
                        break;
                    default:
                        Console.Error.WriteLine($"Unrecognized argument {args[i]}");
                        return false;
                }
                i++;
            }

            if (String.IsNullOrEmpty(cluster))
            {
                Console.Error.WriteLine("Cluster missing");
                return false;
            }

            if (String.IsNullOrEmpty(database))
            {
                Console.Error.WriteLine("Database missing");
                return false;
            }

            if (String.IsNullOrEmpty(table))
            {
                Console.Error.WriteLine("Table missing");
                return false;
            }

            return true;
        }

        static void Main(string[] args)
        {
            if (!ParseCommandLine(args, out var cluster, out var database, out var table))
            {
                Usage();
                return;
            }

            var kcsb = new KustoConnectionStringBuilder
            {
                DataSource = cluster
            };

            kcsb = kcsb.WithAadUserPromptAuthentication();

            CreateJsonMappingIfNotExists(kcsb, database, table);

            // Do ingestion using Kusto.Data client library 
            using (var siClient = KustoClientFactory.CreateCslStreamIngestClient(kcsb))
            {
                using (var data = CreateSampleEventLogCsvStream(10))
                {
                    siClient.ExecuteStreamIngest(database, table, data);
                }

                using (var data = CreateSampleEventLogJsonStream(10))
                {
                    siClient.ExecuteStreamIngestAsync(
                        database, 
                        table, 
                        data, 
                        null, 
                        Kusto.Data.Common.DataSourceFormat.json, 
                        compressStream: false,
                        mappingName: s_jsonMappingName);
                }
            }

            // Do ingestion using Kusto.Ingest client library. The data still goes directly to the engine cluster
            // Just a convenience for applications already using IKustoIngest interface
            using (var ingestClient = KustoIngestFactory.CreateStreamingIngestClient(kcsb))
            {
                using (var data = CreateSampleEventLogCsvStream(10))
                {
                    var ingestProperties = new KustoIngestionProperties(database, table)
                    {
                        Format = DataSourceFormat.csv,
                    };
                    ingestClient.IngestFromStreamAsync(data, ingestProperties);
                }

                using (var data = CreateSampleEventLogJsonStream(10))
                {
                    var ingestProperties = new KustoIngestionProperties(database, table)
                    {
                        Format = DataSourceFormat.json,
                        JSONMappingReference = s_jsonMappingName
                    };

                    ingestClient.IngestFromStreamAsync(data, ingestProperties);
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

                var createMappingCommand = CslCommandGenerator.GenerateTableJsonMappingCreateCommand(tableName, s_jsonMappingName, s_jsonMapping);
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
                    tw.WriteLine("{0},{1},{2},{3}", DateTime.Now, i, "Sample event text ddddddddddddd", "\"{'Prop1':1, 'Prop2': 'Text'}\"");
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
