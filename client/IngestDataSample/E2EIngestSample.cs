using Kusto.Data;
using Kusto.Ingest;
using System;
using System.Data;
using System.Linq;
using System.Threading;
using Kusto.Cloud.Platform.Utils;

namespace IngestDataSample
{
    class E2EIngestSample
    {
        const string SampleDatabaseName = "IngestDataSampleDB";
        // Set the timeout to 6 minutes
        const int TimeOutInMilliSeconds = 360000;
        public static void Run(string dbName = null)
        {

            // This sample code shows several ways of ingesting data into Kusto.
            // The ingestion is done into the Kustolab cluster.

            // Use the provided dbName name or the default Sample Database,
            // and create a matching connection string builder.
            var databaseName = string.IsNullOrEmpty(dbName) ? SampleDatabaseName : dbName;
            Console.WriteLine("Running Ingest Sample on KustoLab on DB:{0}", databaseName);
            var tableName = "MyTable_" + Guid.NewGuid().ToString("N");

            var engineKustoConnectionStringBuilder = new KustoConnectionStringBuilder("https://kustolab.kusto.windows.net/")
            {
                FederatedSecurity = true,
                InitialCatalog = databaseName,
            };

            // Create a connection string builder for KustoLab's DM that will be used for queued ingestion
            var dmKustoConnectionStringBuilderDM = new KustoConnectionStringBuilder(@"https://ingest-kustolab.kusto.windows.net")
            {
                FederatedSecurity = true,
                InitialCatalog = "NetDefaultDB",
            };

            // Create an admin provider through which to send admin commands.
            // (Admin commands all start with a dot, e.g. ".show databases".)
            using (var adminProvider = Kusto.Data.Net.Client.KustoClientFactory.CreateCslAdminProvider(engineKustoConnectionStringBuilder))
            {
                try
                {
                    // Create the sample table
                   var cmd = Kusto.Data.Common.CslCommandGenerator.GenerateTableCreateCommand(tableName, new[] { Tuple.Create("a", "System.String"), Tuple.Create("b", "System.Int32") });
                    adminProvider.ExecuteControlCommand(cmd);

                    // With everything now set up, we can ingest data to the cluster
                    IngestData(engineKustoConnectionStringBuilder, dmKustoConnectionStringBuilderDM, databaseName, tableName);

                    // Delete the sample table
                    cmd = Kusto.Data.Common.CslCommandGenerator.GenerateTableDropCommand(tableName);
                    try
                    {
                        adminProvider.ExecuteControlCommand(cmd);
                    }
                    catch { }

                }
                catch (Exception e)
                {
                   Console.WriteLine("An exception was thrown:'{0}'",e.Message);
                }

            }
        }

        static void IngestData(KustoConnectionStringBuilder engineKustoConnectionStringBuilder, KustoConnectionStringBuilder dmKustoConnectionStringBuilderDM, 
            string databaseName, string tableName)
        {

            // 1. Ingest by connecting directly to the Kustolab cluster and sending a command
            using (IKustoIngestClient directClient = KustoIngestFactory.CreateDirectIngestClient(engineKustoConnectionStringBuilder))
            {
                var kustoIngestionProperties = new KustoIngestionProperties(databaseName, tableName);
                directClient.IngestFromDataReaderAsync(GetDataAsIDataReader(), kustoIngestionProperties);
            }

            // 2. Ingest by submitting the data to the Kustolab ingestion cluster
            //    Note that this is an async operation, so data might not appear immediately
            using (IKustoIngestClient queuedClient = KustoIngestFactory.CreateQueuedIngestClient(dmKustoConnectionStringBuilderDM))
            {
                var kustoIngestionProperties = new KustoIngestionProperties(databaseName, tableName);
                queuedClient.IngestFromDataReaderAsync(GetDataAsIDataReader(), kustoIngestionProperties);
            }

            // 3. Ingest by submitting the data to the Kustolab ingestion cluster - 
            // This time, update the report method and level so you can track the status of your ingestion
            // using the IKustoIngestionResult returned from the ingest operation.
            IKustoIngestionResult ingestionResult;
            using (IKustoIngestClient queuedClient = KustoIngestFactory.CreateQueuedIngestClient(dmKustoConnectionStringBuilderDM))
            {
                var kustoIngestionProperties = new KustoQueuedIngestionProperties(databaseName, tableName)
                {
                    // The default ReportLevel is set to FailuresOnly. 
                    // In this case we want to check the status of successful ingestions as well.
                    ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                    // You can use either a queue or a table to track the status of your ingestion.
                    ReportMethod = IngestionReportMethod.Table
                };
                ingestionResult = queuedClient.IngestFromDataReaderAsync(GetDataAsIDataReader(), kustoIngestionProperties).Result;
            }

            // Obtain the status of our ingestion
            var ingestionStatus = ingestionResult.GetIngestionStatusCollection().First();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            var shouldContinue = true;
            while ((ingestionStatus.Status == Status.Pending) && (shouldContinue))
            {
                // Wait a minute...
                Thread.Sleep(TimeSpan.FromMinutes(1));
                // Try again
                ingestionStatus = ingestionResult.GetIngestionStatusBySourceId(ingestionStatus.IngestionSourceId);
                shouldContinue = watch.ElapsedMilliseconds < TimeOutInMilliSeconds;
            }
            watch.Stop();

            if (ingestionStatus.Status == Status.Pending)
            {
                // The status of the ingestion did not change.
                Console.WriteLine(
                    "Ingestion with ID:'{0}' did not complete. Timed out after :'{1}' Milliseconds"
                        .FormatWithInvariantCulture(
                            ingestionStatus.IngestionSourceId, watch.ElapsedMilliseconds));
            }
            else
            {
                // The status of the ingestion has changed
                Console.WriteLine(
                    "Ingestion with ID:'{0}' is complete. Ingestion Status:'{1}'".FormatWithInvariantCulture(
                        ingestionStatus.IngestionSourceId, ingestionStatus.Status));
            }


            // 4. Show the contents of the table
            using (var client = Kusto.Data.Net.Client.KustoClientFactory.CreateCslQueryProvider(engineKustoConnectionStringBuilder))
            {
                while (true)
                {
                    var query = string.Format("{0}", tableName);
                    var reader = client.ExecuteQuery(query);
                    Kusto.Cloud.Platform.Data.ExtendedDataReader.WriteAsText(reader, "Data ingested into the table:", tabify: true, firstOnly: true);

                    Console.WriteLine("Press 'r' to retry retrieving data from the table, any other key to quit");
                    var key = Console.ReadKey();
                    if (key.KeyChar != 'r' && key.KeyChar != 'R')
                    {
                        break;
                    }
                    Console.WriteLine();
                }
            }
        }

        static IDataReader GetDataAsIDataReader()
        {
            var data = new Row[]
            {
                new Row("hello", 0),
                new Row("world", 1)
            };
            var ret = new Kusto.Cloud.Platform.Data.EnumerableDataReader<Row>(data, "a", "b");
            return ret;
        }

    }

    // A strongly-typed .NET type corresponding to the Kusto table type we ingest into
    class Row
    {
        public string a;
        public int b;

        public Row(string a, int b)
        {
            this.a = a;
            this.b = b;
        }
    }
}
