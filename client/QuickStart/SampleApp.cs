using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;

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
        private static int _step = 1;
        private static bool _WaitForUser;

        /// <summary>
        /// Main Engine and starting point fo the program.
        /// </summary>
        public static async Task Main()
        {
            Console.WriteLine("Kusto sample app is starting...");

            var config = Util.LoadConfigs(ConfigFileName);
            _WaitForUser = config.WaitForUser;

            if (config.AuthenticationMode == "UserPrompt")
                WaitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.");

            var kustoConnectionString = Util.GenerateConnectionString(config.KustoUri, config.AuthenticationMode, config.CertificatePath, config.CertificatePassword, config.ApplicationId, config.TenantId);
            var ingestConnectionString = Util.GenerateConnectionString(config.IngestUri, config.AuthenticationMode, config.CertificatePath, config.CertificatePassword, config.ApplicationId, config.TenantId);

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

        /// <summary>
        /// Basic Table Commands - including AlterMerge Existing Table and Create Table control commands, and Existing Number Of Rows query.
        /// </summary>
        /// <param name="config">ConfigJson object</param>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="queryProvider">Client to run queries</param>
        private static async Task CreateAlterOrQueryTable(ConfigJson config, ICslAdminProvider adminClient, ICslQueryProvider queryProvider)
        {
            if (config.UseExistingTable)
            {
                if (config.AlterTable)
                    // Tip: Usually table was originally created with a schema appropriate for the data being ingested, so this wouldn't be needed.
                    // Learn More: For more information about altering table schemas, see:
                    // https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                    await AlterMergeExistingTableToProvidedSchema(adminClient, config.DatabaseName, config.TableName, config.TableSchema);

                if (config.QueryData)
                    // Learn More: For more information about Kusto Query Language (KQL), see: https://docs.microsoft.com/azure/data-explorer/write-queries
                    await QueryExistingNumberOfRows(queryProvider, config.DatabaseName, config.TableName);
            }
            else
                // Tip: This is generally a one-time configuration
                // Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
                await CreateNewTable(adminClient, config.DatabaseName, config.TableName, config.TableSchema, config.BatchingPolicy);
        }

        /// <summary>
        /// Alter-merges the given existing table to provided schema.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="configTableSchema">Table Schema</param>
        private static async Task AlterMergeExistingTableToProvidedSchema(ICslAdminProvider adminClient, string configDatabaseName, string configTableName, string configTableSchema)
        {
            WaitForUserToProceed($"Alter-merge existing table '{configDatabaseName}.{configTableName}' to align with the provided schema");

            // You can also use the CslCommandGenerator class to build commands: string command = CslCommandGenerator.GenerateTableAlterMergeCommand();
            var command = $".alter-merge table {configTableName} {configTableSchema}";

            if (!await Util.ExecuteControlCommand(adminClient, configDatabaseName, command))
                Util.ErrorHandler($"Failed to alter table using command '{command}'");
        }

        /// <summary>
        /// Queries the data on the existing number of rows.
        /// </summary>
        /// <param name="queryClient">Client to run queries</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        private static async Task QueryExistingNumberOfRows(ICslQueryProvider queryClient, string configDatabaseName, string configTableName)
        {
            WaitForUserToProceed($"Get existing row count in '{configDatabaseName}.{configTableName}'");
            var query = $"{configTableName} | count";
            if (!await Util.ExecuteQuery(queryClient, configDatabaseName, query))
                Util.ErrorHandler($"Failed to execute query: '{query}'");
        }

        /// <summary>
        /// Creates a new table.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="configTableSchema">Table Schema</param>
        /// <param name="batchingPolicy">Ingestion batching policy</param>
        private static async Task CreateNewTable(ICslAdminProvider adminClient, string configDatabaseName, string configTableName, string configTableSchema, string batchingPolicy)
        {
            WaitForUserToProceed($"Create table '{configDatabaseName}.{configTableName}'");

            // You can also use the CslCommandGenerator class to build commands: string command = CslCommandGenerator.GenerateTableCreateCommand();
            var command = $".create table {configTableName} {configTableSchema}";

            if (!await Util.ExecuteControlCommand(adminClient, configDatabaseName, command))
                Util.ErrorHandler($"Failed to create table or validate it exists using command '{command}'");

            // Learn More: Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
            //   1) More than 1,000 files were queued for ingestion for the same table by the same user
            //   2) More than 1GB of data was queued for ingestion for the same table by the same user
            //   3) More than 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
            //  For more information about customizing the ingestion batching policy, see:
            // https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy

            // TODO: Change if needed.
            // Disabled to prevent an existing batching policy from being unintentionally changed
            if (false && !String.IsNullOrEmpty(batchingPolicy))
                await AlterBatchingPolicy(adminClient, configDatabaseName, configTableName, batchingPolicy);
        }

        /// <summary>
        /// Alters the batching policy based on BatchingPolicy in configuration.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="batchingPolicy">Ingestion batching policy</param>
        private static async Task AlterBatchingPolicy(ICslAdminProvider adminClient, string configDatabaseName, string configTableName, string batchingPolicy)
        {
            // Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and in this sample app, we opt to
            // modify the default ingestion policy to ingest data after at most 10 seconds.
            // Tip 2: This is generally a one-time configuration.
            // Tip 3: You can also skip the batching for some files using the Flush-Immediately property, though this option should be used with care as it is
            // inefficient.

            WaitForUserToProceed($"Alter the batching policy for table '{configDatabaseName}.{configTableName}'");
            var command = $".alter table {configTableName} policy ingestionbatching @'{batchingPolicy}'";
            if (!await Util.ExecuteControlCommand(adminClient, configDatabaseName, command))
                Console.WriteLine("Failed to alter the ingestion policy, which could be the result of insufficient permissions. The sample will still run, " +
                                  "though ingestion will be delayed for up to 5 minutes.");
        }

        /// <summary>
        /// Entire ingestion process.
        /// </summary>
        /// <param name="config">ConfigJson object</param>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="ingestClient">Client to ingest data</param>
        private static async Task IngestionHandlerAsync(ConfigJson config, ICslAdminProvider adminClient, IKustoIngestClient ingestClient)
        {
            foreach (var file in config.Data)
            {
                var dataFormat = (DataSourceFormat)Enum.Parse(typeof(DataSourceFormat), file["format"].ToLower());
                var mappingName = file["mappingName"];

                // Tip: This is generally a one-time configuration. Learn More: For more information about providing inline mappings and mapping references,
                // see: https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
                if (!await CreateIngestionMappings(bool.Parse(file["useExistingMapping"]), adminClient, config.DatabaseName, config.TableName,
                        mappingName, file["mappingValue"], dataFormat))
                    continue;

                // Learn More: For more information about ingesting data to Kusto in C#,
                // see: https://docs.microsoft.com/en-us/azure/data-explorer/net-sdk-ingest-data
                await IngestAsync(file, dataFormat, ingestClient, config.DatabaseName, config.TableName, mappingName);
            }

            await Util.WaitForIngestionToComplete(config.WaitForIngestSeconds);
        }

        /// <summary>
        /// Creates Ingestion Mappings (if required) based on given values.
        /// </summary>
        /// <param name="useExistingMapping">Flag noting if we should the existing mapping or create a new one</param>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="mappingName">Desired mapping name</param>
        /// <param name="mappingValue">Values of the new mappings to create</param>
        /// <param name="dataFormat">Given data format</param>
        /// <returns>True if Ingestion Mappings exists (whether by us, or the already existing one)</returns>
        private static async Task<bool> CreateIngestionMappings(bool useExistingMapping, ICslAdminProvider adminClient, string configDatabaseName, string configTableName, string mappingName, string mappingValue, DataSourceFormat dataFormat)
        {
            if (useExistingMapping || mappingValue is null)
                return true;

            var ingestionMappingKind = dataFormat.ToIngestionMappingKind().ToString().ToLower();
            WaitForUserToProceed($"Create a '{ingestionMappingKind}' mapping reference named '{mappingName}'");

            mappingName = mappingName ?? "DefaultQuickstartMapping" + Guid.NewGuid().ToString().Substring(0, 5);
            var mappingCommand =
                $".create-or-alter table {configTableName} ingestion {ingestionMappingKind} mapping '{mappingName}' '{mappingValue}'";

            if (!await Util.ExecuteControlCommand(adminClient, configDatabaseName, mappingCommand))
                Util.ErrorHandler(
                    $"Failed to create a '{ingestionMappingKind}' mapping reference named '{mappingName}'. Skipping this ingestion.");

            return true;
        }

        /// <summary>
        /// Ingest data from given source.
        /// </summary>
        /// <param name="dataSource">Given data source</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="ingestClient">Client to ingest data</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="mappingName">Desired mapping name</param>
        private static async Task IngestAsync(IReadOnlyDictionary<string, string> dataSource, DataSourceFormat dataFormat, IKustoIngestClient ingestClient, string configDatabaseName, string configTableName, string mappingName)
        {
            var sourceType = dataSource["sourceType"].ToLower();
            var sourceUri = dataSource["dataSourceUri"];
            WaitForUserToProceed($"Ingest '{sourceUri}' from '{sourceType}'");

            // Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
            // If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
            dataFormat = dataFormat == DataSourceFormat.json ? DataSourceFormat.multijson : dataFormat;

            // Tip: Kusto's C# SDK can ingest data from files, blobs and open streams.See the SDK's samples and the E2E tests in azure.kusto.ingest for
            // additional references.

            switch (sourceType)
            {
                case "localfilesource":
                    await Util.IngestFromFile(ingestClient, configDatabaseName, configTableName, sourceUri, dataFormat, mappingName);
                    break;
                case "blobsource":
                    await Util.IngestFromBlobAsync(ingestClient, configDatabaseName, configTableName, sourceUri, dataFormat, mappingName);
                    break;
                default:
                    Util.ErrorHandler($"Unknown source '{sourceType}' for file '{sourceUri}'");
                    break;
            }
        }

        /// <summary>
        /// End-Of-Script simple queries, to validate the hopefully successful run of the script.  
        /// </summary>
        /// <param name="queryProvider">Client to run queries</param>
        /// <param name="configDatabaseName">DB Name</param>
        /// <param name="configTableName">Table Name</param>
        /// <param name="configIngestData">Flag noting whether any data was ingested by the script</param>
        private static async Task ExecuteValidationQueries(ICslQueryProvider queryProvider, string configDatabaseName, string configTableName, bool configIngestData)
        {
            var optionalPostIngestionPrompt = configIngestData ? "post-ingestion " : "";
            WaitForUserToProceed($"Get {optionalPostIngestionPrompt}row count for '{configDatabaseName}.{configTableName}':");

            var rowQuery = $"{configTableName} | count";
            if (!await Util.ExecuteQuery(queryProvider, configDatabaseName, rowQuery))
                Util.ErrorHandler($"Failed to execute query: '{rowQuery}'");

            WaitForUserToProceed($"Get sample (2 records) of {optionalPostIngestionPrompt}data:");

            var sampleQuery = $"{configTableName} | take 2";
            if (!await Util.ExecuteQuery(queryProvider, configDatabaseName, sampleQuery))
                Util.ErrorHandler($"Failed to execute query: '{sampleQuery}'");
        }

        /// <summary>
        /// Handles UX on prompts and flow of program 
        /// </summary>
        /// <param name="promptMsg"> Prompt to display to user.</param>
        private static void WaitForUserToProceed(string promptMsg)
        {
            Console.WriteLine($"\nStep {_step}: {promptMsg}");
            _step++;
            if (_WaitForUser)
            {
                Console.WriteLine("Press any key to proceed with this operation...");
                Console.ReadLine();
            }
        }
    }
}