using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using Newtonsoft.Json;
using Kusto.Cloud.Platform.Data;
using ShellProgressBar;


namespace QuickStart
{
    /// <summary>
    /// The quick start application is a self-contained and runnable example script that demonstrates authenticating connecting to, administering, ingesting
    /// data into and querying Azure Data Explorer using the azure-kusto C# SDK. You can use it as a baseline to write your own first kusto client application,
    /// altering the code as you go, or copy code sections out of it into your app.
    /// Tip: The app includes comments with tips on recommendations, coding best practices, links to reference materials and recommended TO DO changes when
    /// adapting the code to your needs.
    /// </summary>
    public static class KustoSampleApp
    {
        #region Constans

        // TODO (config):
        // If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details.
        // If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately.
        private const string ConfigFileName = @"kusto_sample_config.json";

        #endregion

        public static async Task Main()
        {
            Console.WriteLine("Kusto sample app is starting...");
            var config = Util.LoadConfigs(ConfigFileName);

            if (config.AuthenticationMode == "UserPrompt")
                WaitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.");

            var kustoConnectionString = GenerateConnectionString(config.KustoUri, config.AuthenticationMode, config.CertificatePath, config.CertificatePassword,
                config.ApplicationId, config.TenantId);
            var ingestConnectionString = GenerateConnectionString(config.IngestUri, config.AuthenticationMode, config.CertificatePath, config.CertificatePassword,
                config.ApplicationId, config.TenantId);


            using (var adminClient = KustoClientFactory.CreateCslAdminProvider(kustoConnectionString))
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider(kustoConnectionString))
            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConnectionString))
            {
                await CreateAlterOrQueryTable(config, adminClient, queryProvider);
                if (config.IngestData)
                    await IngestionHandlerAsync(config, adminClient, ingestClient);
                if (config.QueryData)
                    await ExecuteValidationQueries(queryProvider, config.DatabaseName, config.TableName, config.IngestData);
            }
            Console.WriteLine("\nKusto sample app done");
        }


    }
}