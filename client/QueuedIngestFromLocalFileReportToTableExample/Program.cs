using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace QueuedIngestFromLocalFileReportToTableExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Ingest From a Local File using KustoQueuedIngestClient and report status to a table

            // Create Kusto connection string with App Authentication
            var kustoConnectionStringBuilderDM =
                new KustoConnectionStringBuilder(@"https://ingest-{clusterNameAndRegion}.kusto.windows.net").WithAadApplicationKeyAuthentication(
                    applicationClientId: "{Application Client ID}",
                    applicationKey: "{Application Key (secret)}",
                    authority: "{AAD TenantID or name}");

            // Create a disposable client that will execute the ingestion
            IKustoQueuedIngestClient client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);

            // Ingest from a file according to the required properties
            var kustoIngestionProperties = new KustoQueuedIngestionProperties(databaseName: "myDB", tableName: "myDB")
            {
                // Setting the report level to FailuresAndSuccesses will cause both successful and failed ingestions to be reported
                // (Rather than the default "FailuresOnly" level)
                ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                // Choose the report method of choice
                ReportMethod = IngestionReportMethod.Table
            };

            var filePath = @"< Path to file >";
            var fileIdentifier = Guid.NewGuid();
            var sourceOptions = new StorageSourceOptions() { SourceId = fileIdentifier };

            // Execute the ingest operation and save the result.
            var clientResult = await client.IngestFromStorageAsync(filePath,
                ingestionProperties: kustoIngestionProperties, sourceOptions);

            // Use the fileIdentifier you supplied to get the status of your ingestion 
            var ingestionStatus = clientResult.GetIngestionStatusBySourceId(fileIdentifier);
            while (ingestionStatus.Status == Status.Pending)
            {
                // Wait a minute...
                Thread.Sleep(TimeSpan.FromMinutes(1));
                // Try again
                ingestionStatus = clientResult.GetIngestionStatusBySourceId(fileIdentifier);
            }

            // Verify the results of the ingestion
            Ensure.ConditionIsMet(ingestionStatus.Status == Status.Succeeded,
                "The file should have been ingested successfully");

            // Dispose of the client
            client.Dispose();
        }
    }
}
