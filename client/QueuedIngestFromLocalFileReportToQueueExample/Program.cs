using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace QueuedIngestFromLocalFileReportToQueueExample
{
    class Program
    {
        static void Main(string[] args)
        {
            // Ingest From a Local Files using KustoQueuedIngestClient and report status to a queue

            // Create Kusto connection string with App Authentication
            var kustoConnectionStringBuilderDM =
                new KustoConnectionStringBuilder(@"https://ingest-{clusterNameAndRegion}.kusto.windows.net").WithAadApplicationKeyAuthentication(
                    applicationClientId: "{Application Client ID}",
                    applicationKey: "{Application Key (secret)}",
                    authority: "{AAD TenantID or name}");

            // Create a disposable client that will execute the ingestion
            IKustoQueuedIngestClient client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);

            // Ingest from a file according to the required properties
            var kustoIngestionProperties = new KustoQueuedIngestionProperties(databaseName: "myDB", tableName: "myTable")
            {
                // Setting the report level to FailuresAndSuccesses will cause both successful and failed ingestions to be reported
                // (Rather than the default "FailuresOnly" level - which is demonstrated in the
                // 'Ingest From Local File(s) using KustoQueuedIngestClient and Ingestion Validation' section)
                ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                // Choose the report method of choice. 'Queue' is the default method.
                // For the sake of the example, we will choose it anyway. 
                ReportMethod = IngestionReportMethod.Queue
            };

            client.IngestFromStorageAsync("ValidTestFile.csv", kustoIngestionProperties);
            client.IngestFromStorageAsync("InvalidTestFile.csv", kustoIngestionProperties);

            // Waiting for the aggregation
            Thread.Sleep(TimeSpan.FromMinutes(8));

            // Retrieve and validate failures
            var ingestionFailures = client.PeekTopIngestionFailuresAsync().GetAwaiter().GetResult();
            Ensure.IsTrue((ingestionFailures.Count() > 0), "The failed ingestion should have been reported to the failed ingestions queue");
            // Retrieve, delete and validate failures
            ingestionFailures = client.GetAndDiscardTopIngestionFailuresAsync().GetAwaiter().GetResult();
            Ensure.IsTrue((ingestionFailures.Count() > 0), "The failed ingestion should have been reported to the failed ingestions queue");

            // Verify the success has also been reported to the queue
            var ingestionSuccesses = client.GetAndDiscardTopIngestionSuccessesAsync().GetAwaiter().GetResult();
            Ensure.ConditionIsMet((ingestionSuccesses.Count() > 0),
                "The successful ingestion should have been reported to the successful ingestions queue");

            // Dispose of the client
            client.Dispose();
        }
    }
}
