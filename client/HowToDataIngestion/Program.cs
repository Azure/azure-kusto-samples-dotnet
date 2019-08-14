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
using System.Threading;
using System.Threading.Tasks;

namespace HowToDataIngestion
{
    class Program
    {
        static void Main(string[] args)
        {
            var clusterName = "KustoLab";
            var db = "KustoIngestClientDemo";
            var table = "Table1";
            var mappingName = "Table1_mapping_1";
            var serviceNameAndRegion = "clusterNameAndRegion"; // For example, "mycluster.westus"
            var authority = "AAD Tenant or name"; // For example, "microsoft.com"

            // Set up table
            var kcsbEngine =
                new KustoConnectionStringBuilder($"https://{serviceNameAndRegion}.kusto.windows.net").WithAadUserPromptAuthentication(authority: $"{authority}");

            using (var kustoAdminClient = KustoClientFactory.CreateCslAdminProvider(kcsbEngine))
            {
                var columns = new List<Tuple<string, string>>()
                {
                    new Tuple<string, string>("Column1", "System.Int64"),
                    new Tuple<string, string>("Column2", "System.DateTime"),
                    new Tuple<string, string>("Column3", "System.String"),
                };

                var command = CslCommandGenerator.GenerateTableCreateCommand(table, columns);
                kustoAdminClient.ExecuteControlCommand(databaseName: db, command: command);

                // Set up mapping
                var columnMappings = new List<JsonColumnMapping>();
                columnMappings.Add(new JsonColumnMapping()
                { ColumnName = "Column1", JsonPath = "$.Id" });
                columnMappings.Add(new JsonColumnMapping()
                { ColumnName = "Column2", JsonPath = "$.Timestamp" });
                columnMappings.Add(new JsonColumnMapping()
                { ColumnName = "Column3", JsonPath = "$.Message" });

                command = CslCommandGenerator.GenerateTableJsonMappingCreateCommand(
                                                    table, mappingName, columnMappings);
                kustoAdminClient.ExecuteControlCommand(databaseName: db, command: command);
            }

            // Create Ingest Client
            var kcsbDM =
                new KustoConnectionStringBuilder($"https://ingest-{serviceNameAndRegion}.kusto.windows.net").WithAadUserPromptAuthentication(authority: $"{authority}");

            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(kcsbDM))
            {
                var ingestProps = new KustoQueuedIngestionProperties(db, table);
                // For the sake of getting both failure and success notifications we set this to IngestionReportLevel.FailuresAndSuccesses
                // Usually the recommended level is IngestionReportLevel.FailuresOnly
                ingestProps.ReportLevel = IngestionReportLevel.FailuresAndSuccesses;
                ingestProps.ReportMethod = IngestionReportMethod.Queue;
                // Setting FlushImmediately to 'true' overrides any aggregation preceding the ingestion.
                // Not recommended unless you are certain you know what you are doing
                ingestProps.FlushImmediately = true;
                ingestProps.JSONMappingReference = mappingName;
                ingestProps.Format = DataSourceFormat.json;

                // Prepare data for ingestion
                using (var memStream = new MemoryStream())
                using (var writer = new StreamWriter(memStream))
                {
                    for (int counter = 1; counter <= 10; ++counter)
                    {
                        writer.WriteLine(
                            "{{ \"Id\":\"{0}\", \"Timestamp\":\"{1}\", \"Message\":\"{2}\" }}",
                            counter, DateTime.UtcNow.AddSeconds(100 * counter),
                            $"This is a dummy message number {counter}");
                    }

                    writer.Flush();
                    memStream.Seek(0, SeekOrigin.Begin);

                    // Post ingestion message
                    var res = ingestClient.IngestFromStreamAsync(memStream, ingestProps, leaveOpen: true);
                }

                // Wait a bit (20s) and retrieve all notifications:
                Thread.Sleep(20000);
                var errors = ingestClient.GetAndDiscardTopIngestionFailures().GetAwaiter().GetResult();
                var successes = ingestClient.GetAndDiscardTopIngestionSuccesses().GetAwaiter().GetResult();

                errors.ForEach((f) => { Console.WriteLine($"Ingestion error: {f.Info.Details}"); });
                successes.ForEach((s) => { Console.WriteLine($"Ingested: {s.Info.IngestionSourcePath}"); });
            }
        }
    }
}
