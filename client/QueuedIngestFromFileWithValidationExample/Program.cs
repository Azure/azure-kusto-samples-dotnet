using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace QueuedIngestFromFileWithValidationExample
{
    class Program
    {
        static void Main(string[] args)
        {
            // Ingest From Local Files using KustoQueuedIngestClient and Ingestion Validation

            // Create Kusto connection string with App Authentication
            var kustoConnectionStringBuilderDM =
                new KustoConnectionStringBuilder(@"https://ingest-{clusterNameAndRegion}.kusto.windows.net").WithAadApplicationKeyAuthentication(
                    applicationClientId: "{Application Client ID}",
                    applicationKey: "{Application Key (secret)}",
                    authority: "{AAD TenantID or name}");

            // Create a disposable client that will execute the ingestion
            IKustoQueuedIngestClient client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);

            // Ingest from files according to the required properties
            var kustoIngestionProperties = new KustoIngestionProperties(databaseName: "myDB", tableName: "myTable");

            client.IngestFromStorageAsync(@"ValidTestFile.csv", kustoIngestionProperties);
            client.IngestFromStorageAsync(@"InvalidTestFile.csv", kustoIngestionProperties);

            // Waiting for the aggregation
            Thread.Sleep(TimeSpan.FromMinutes(8));

            // Retrieve and validate failures
            var ingestionFailures = client.PeekTopIngestionFailuresAsync().GetAwaiter().GetResult();
            Ensure.IsTrue((ingestionFailures.Count() > 0), "Failures expected");
            // Retrieve, delete and validate failures
            ingestionFailures = client.GetAndDiscardTopIngestionFailuresAsync().GetAwaiter().GetResult();
            Ensure.IsTrue((ingestionFailures.Count() > 0), "Failures expected");

            // Dispose of the client
            client.Dispose();
        }
    }
}
