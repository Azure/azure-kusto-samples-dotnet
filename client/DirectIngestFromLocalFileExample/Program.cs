using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DirectIngestFromLocalFileExample
{
    class Program
    {
        static void Main(string[] args)
        {
            // Ingest From Local File using KustoDirectIngestClient (only for test purposes):

            // Create Kusto connection string with App Authentication
            var kustoConnectionStringBuilderEngine =
                new KustoConnectionStringBuilder(@"https://{clusterNameAndRegion}.kusto.windows.net").WithAadApplicationKeyAuthentication(
                    applicationClientId: "{Application Client ID}",
                    applicationKey: "{Application Key (secret)}",
                    authority: "{AAD TenantID or name}");

            // Create a disposable client that will execute the ingestion
            using (IKustoIngestClient client = KustoIngestFactory.CreateDirectIngestClient(kustoConnectionStringBuilderEngine))
            {
                //Ingest from blobs according to the required properties
                var kustoIngestionProperties = new KustoIngestionProperties(databaseName: "myDB", tableName: "myTable");

                client.IngestFromStorageAsync(@"< Path to local file >", ingestionProperties: kustoIngestionProperties).GetAwaiter().GetResult();
            }
        }
    }
}
