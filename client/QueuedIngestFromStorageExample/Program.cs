using Kusto.Data;
using Kusto.Ingest;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueuedIngestFromStorageExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Async Ingestion From a Single Azure Blob using KustoQueuedIngestClient with (optional) RetryPolicy:

            //Create Kusto connection string with App Authentication
            var kustoConnectionStringBuilderDM =
                new KustoConnectionStringBuilder(@"https://ingest-{clusterNameAndRegion}.kusto.windows.net").WithAadApplicationKeyAuthentication(
                    applicationClientId: "{Application Client ID}",
                    applicationKey: "{Application Key (secret)}",
                    authority: "{AAD TenantID or name}");

            // Create an ingest client
            // Note, that creating a separate instance per ingestion operation is an anti-pattern.
            // IngestClient classes are thread-safe and intended for reuse
            IKustoIngestClient client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);

            // Ingest from blobs according to the required properties
            var kustoIngestionProperties = new KustoIngestionProperties(databaseName: "myDB", tableName: "myTable");

            var sourceOptions = new StorageSourceOptions() { DeleteSourceOnSuccess = true };

            // Create your custom implementation of IRetryPolicy, which will affect how the ingest client handles retrying on transient failures
            IRetryPolicy retryPolicy = new NoRetry();
            
            // This line sets the retry policy on the ingest client that will be enforced on every ingest call from here on
            ((IKustoQueuedIngestClient)client).QueueOptions.QueueRequestOptions.RetryPolicy = retryPolicy;

            await client.IngestFromStorageAsync(uri: @"BLOB-URI-WITH-SAS-KEY", ingestionProperties: kustoIngestionProperties, sourceOptions);

            client.Dispose();
        }
    }
}
