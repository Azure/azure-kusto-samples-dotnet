using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using Newtonsoft.Json;

namespace QuickStart
{

    /// <summary>
    /// ConfigJson object - represents a cluster and DataBase connection configuration file.
    /// </summary>
    public class ConfigJson
    {
        /// Flag to indicate whether to use an existing table, or to create a new one. 
        public bool UseExistingTable { get; set; }

        /// DB to work on from the given URI.
        public string DatabaseName { get; set; }

        /// Table to work on from the given DB.
        public string TableName { get; set; }

        /// Table to work with from the given Table.
        public string TableSchema { get; set; }

        /// Cluster to connect to and query from.
        public string KustoUri { get; set; }

        /// Ingestion cluster to connect and ingest to. will usually be the same as the KustoUri, but starting with "ingest-"...
        public string IngestUri { get; set; }

        /// Certificate Path when using AppCertificate authentication mode.
        public string CertificatePath { get; set; }

        /// Certificate Password when using AppCertificate authentication mode.
        public string CertificatePassword { get; set; }

        /// Application Id when using AppCertificate authentication mode.
        public string ApplicationId { get; set; }

        /// Tenant Id when using AppCertificate authentication mode.
        public string TenantId { get; set; }

        /// Data sources list to ingest from.
        public List<Dictionary<string, string>> Data { get; set; }

        /// Flag to indicate whether to Alter-merge the table (query).
        public bool AlterTable { get; set; }

        /// Flag to indicate whether to query the starting data (query).
        public bool QueryData { get; set; }

        /// Flag to indicate whether ingest data based on data sources.
        public bool IngestData { get; set; }

        /// Recommended default: UserPrompt
        /// Some of the auth modes require additional environment variables to be set in order to work (see usage in generate_connection_string function).
        /// Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
        /// Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)
        public string AuthenticationMode { get; set; }

        /// Recommended default: True
        /// Toggle to False to execute this script "unattended"
        public bool WaitForUser { get; set; }

        /// Sleep time to allow for queued ingestion to complete.
        public int WaitForIngestSeconds { get; set; }

        /// Optional - Customized ingestion batching policy
        public string BatchingPolicy { get; set; }
    }

    /// <summary>
    /// The quick start application is a self-contained and runnable example script that demonstrates authenticating connecting to, administering, ingesting
    /// data into and querying Azure Data Explorer using the azure-kusto C# SDK. You can use it as a baseline to write your own first kusto client application,
    /// altering the code as you go, or copy code sections out of it into your app.
    /// Tip: The app includes comments with tips on recommendations, coding best practices, links to reference materials and recommended TO DO changes when
    /// adapting the code to your needs.
    /// </summary>
    public static class KustoSampleApp
    {

        // TODO (config):
        // If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details.
        // If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately.
        private const string ConfigFileName = @"kusto_sample_config.json";
        private static int Step = 1;
        private static bool _WaitForUser;

        /// <summary>
        /// Main Engine and starting point fo the program.
        /// </summary>
        public static async Task Main()
        {
            Console.WriteLine("Kusto sample app is starting...");

            var config = LoadConfigs(ConfigFileName);
            _WaitForUser = config.WaitForUser;

            if (config.AuthenticationMode == "UserPrompt")
                WaitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.");

            var kustoConnectionString = Utils.GenerateConnectionString(config.KustoUri, config.AuthenticationMode, config.CertificatePath, config.CertificatePassword, config.ApplicationId, config.TenantId);
            var ingestConnectionString = Utils.GenerateConnectionString(config.IngestUri, config.AuthenticationMode, config.CertificatePath, config.CertificatePassword, config.ApplicationId, config.TenantId);

            using (var adminClient = KustoClientFactory.CreateCslAdminProvider(kustoConnectionString)) // For control commands
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider(kustoConnectionString)) // For regular querying
            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConnectionString)) // For ingestion
            {
                await PreIngestionQueryingAsync(config, adminClient, queryProvider);

                if (config.IngestData)
                    await IngestionAsync(config, adminClient, ingestClient);

                if (config.QueryData)
                    await PostIngestionQueryingAsync(queryProvider, config.DatabaseName, config.TableName, config.IngestData);
            }

            Console.WriteLine("\nKusto sample app done");
        }

        /// <summary>
        /// Loads JSON configuration file, and sets the metadata in place. 
        /// </summary>
        /// <param name="configFilePath"> Configuration file path.</param>
        /// <returns>ConfigJson object, allowing access to the metadata fields.</returns>
        public static ConfigJson LoadConfigs(string configFilePath)
        {
            try
            {
                var json = File.ReadAllText(configFilePath);
                var config = JsonConvert.DeserializeObject<ConfigJson>(json);
                var missing = new[]
                {
                    (name: nameof(config.DatabaseName), value: config.DatabaseName),
                    (name: nameof(config.TableName), value: config.TableName),
                    (name: nameof(config.TableSchema), value: config.TableSchema),
                    (name: nameof(config.KustoUri), value: config.KustoUri),
                    (name: nameof(config.IngestUri), value: config.IngestUri),
                    (name: nameof(config.AuthenticationMode), value: config.AuthenticationMode)
                }.Where(item => string.IsNullOrWhiteSpace(item.value)).ToArray();

                if (missing.Any())
                {
                    Utils.ErrorHandler($"File '{configFilePath}' is missing required fields: {string.Join(", ", missing.Select(item => item.name))}");
                }

                if (config.Data is null || !config.Data.Any() || config.Data[0].Count == 0)
                {
                    Utils.ErrorHandler($"Required field Data in '{configFilePath}' is either missing, empty or misfilled");
                }

                return config;
            }
            catch (Exception ex)
            {
                Utils.ErrorHandler($"Couldn't read config file: '{configFilePath}'", ex);
            }

            throw new InvalidOperationException("Unreachable code");
        }

        /// <summary>
        /// First phase, pre ingestion - will reach the provided DB with several control commands and a query based on the configuration file.
        /// </summary>
        /// <param name="config">ConfigJson object containing the SampleApp configuration</param>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="queryProvider">Client to run queries</param>
        private static async Task PreIngestionQueryingAsync(ConfigJson config, ICslAdminProvider adminClient, ICslQueryProvider queryProvider)
        {
            if (config.UseExistingTable)
            {
                if (config.AlterTable)
                {
                    // Tip: Usually table was originally created with a schema appropriate for the data being ingested, so this wouldn't be needed.
                    // Learn More: For more information about altering table schemas, see:
                    // https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                    WaitForUserToProceed($"Alter-merge existing table '{config.DatabaseName}.{config.TableName}' to align with the provided schema");
                    await AlterMergeExistingTableToProvidedSchemaAsync(adminClient, config.DatabaseName, config.TableName, config.TableSchema);
                }
            }
            else
            {
                // Tip: This is generally a one-time configuration
                // Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
                WaitForUserToProceed($"Create table '{config.DatabaseName}.{config.TableName}'");
                await CreateNewTableAsync(adminClient, config.DatabaseName, config.TableName, config.TableSchema);
            }

            // Learn More: Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
            //   1) More than 1,000 files were queued for ingestion for the same table by the same user
            //   2) More than 1GB of data was queued for ingestion for the same table by the same user
            //   3) More than 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
            //  For more information about customizing the ingestion batching policy, see:
            // https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy
            // TODO: Change if needed. Disabled to prevent an existing batching policy from being unintentionally changed
            if (false && !String.IsNullOrEmpty(config.BatchingPolicy))
            {
                WaitForUserToProceed($"Alter the batching policy for table '{config.DatabaseName}.{config.TableName}'");
                await AlterBatchingPolicyAsync(adminClient, config.DatabaseName, config.TableName, config.BatchingPolicy);
            }

            if (config.QueryData)
            {
                WaitForUserToProceed($"Get existing row count in '{config.DatabaseName}.{config.TableName}'");
                await QueryExistingNumberOfRowsAsync(queryProvider, config.DatabaseName, config.TableName);
            }

        }

        /// <summary>
        /// Alter-merges the given existing table to provided schema.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="configTableSchema">Table Schema</param>
        private static async Task AlterMergeExistingTableToProvidedSchemaAsync(ICslAdminProvider adminClient, string configDatabaseName, string configTableName, string configTableSchema)
        {
            // You can also use the CslCommandGenerator class to build commands: string command = CslCommandGenerator.GenerateTableAlterMergeCommand();
            var command = $".alter-merge table {configTableName} {configTableSchema}";
            await Utils.ExecuteAsync(adminClient, configDatabaseName, command);
        }

        /// <summary>
        /// Creates a new table.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="configTableSchema">Table Schema</param>
        private static async Task CreateNewTableAsync(ICslAdminProvider adminClient, string configDatabaseName, string configTableName, string configTableSchema)
        {
            // You can also use the CslCommandGenerator class to build commands: string command = CslCommandGenerator.GenerateTableCreateCommand();
            var command = $".create table {configTableName} {configTableSchema}";
            await Utils.ExecuteAsync(adminClient, configDatabaseName, command);
        }

        /// <summary>
        /// Alters the batching policy based on BatchingPolicy in configuration.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="batchingPolicy">Ingestion batching policy</param>
        private static async Task AlterBatchingPolicyAsync(ICslAdminProvider adminClient, string configDatabaseName, string configTableName, string batchingPolicy)
        {
            // Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and in this sample app, we opt to modify the default ingestion policy to ingest data after at most 10 seconds.
            // Tip 2: This is generally a one-time configuration.
            // Tip 3: You can also skip the batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient.
            var command = $".alter table {configTableName} policy ingestionbatching @'{batchingPolicy}'";
            await Utils.ExecuteAsync(adminClient, configDatabaseName, command);
            // If it failed to alter the ingestion policy - it could be the result of insufficient permissions. The sample will still run, though ingestion will be delayed for up to 5 minutes.
        }

        /// <summary>
        /// Queries the data on the existing number of rows.
        /// </summary>
        /// <param name="queryClient">Client to run queries</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        private static async Task QueryExistingNumberOfRowsAsync(ICslQueryProvider queryClient, string configDatabaseName, string configTableName)
        {
            var query = $"{configTableName} | count";
            await Utils.ExecuteAsync(queryClient, configDatabaseName, query);
        }

        /// <summary>
        /// Queries the first two rows of the table.
        /// </summary>
        /// <param name="queryClient">Client to run queries</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        private static async Task QueryFirstTwoRowsAsync(ICslQueryProvider queryClient, string configDatabaseName, string configTableName)
        {
            var query = $"{configTableName} | take 2";
            await Utils.ExecuteAsync(queryClient, configDatabaseName, query);
        }

        /// <summary>
        /// Second phase - The ingestion process.
        /// </summary>
        /// <param name="config">ConfigJson object</param>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="ingestClient">Client to ingest data</param>
        private static async Task IngestionAsync(ConfigJson config, ICslAdminProvider adminClient, IKustoIngestClient ingestClient)
        {
            foreach (var file in config.Data)
            {
                var dataFormat = (DataSourceFormat)Enum.Parse(typeof(DataSourceFormat), file["format"].ToLower());
                var mappingName = file["mappingName"];

                // Tip: This is generally a one-time configuration. Learn More: For more information about providing inline mappings and mapping references,
                // see: https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
                if (!await CreateIngestionMappingsAsync(bool.Parse(file["useExistingMapping"]), adminClient, config.DatabaseName, config.TableName, mappingName, file["mappingValue"], dataFormat))
                    continue;

                // Learn More: For more information about ingesting data to Kusto in C#,
                // see: https://docs.microsoft.com/en-us/azure/data-explorer/net-sdk-ingest-data
                await IngestDataAsync(file, dataFormat, ingestClient, config.DatabaseName, config.TableName, mappingName);
            }

            await Utils.WaitForIngestionToCompleteAsync(config.WaitForIngestSeconds);
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
        private static async Task<bool> CreateIngestionMappingsAsync(bool useExistingMapping, ICslAdminProvider adminClient, string configDatabaseName, string configTableName, string mappingName, string mappingValue, DataSourceFormat dataFormat)
        {
            if (useExistingMapping || mappingValue is null)
                return true;

            var ingestionMappingKind = dataFormat.ToIngestionMappingKind().ToString().ToLower();
            WaitForUserToProceed($"Create a '{ingestionMappingKind}' mapping reference named '{mappingName}'");

            mappingName = mappingName ?? "DefaultQuickstartMapping" + Guid.NewGuid().ToString().Substring(0, 5);
            var mappingCommand = $".create-or-alter table {configTableName} ingestion {ingestionMappingKind} mapping '{mappingName}' '{mappingValue}'";

            try
            {
                await Utils.ExecuteAsync(adminClient, configDatabaseName, mappingCommand);
                return true;
            }
            catch (Exception)
            {
                return false;
            }

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
        private static async Task IngestDataAsync(IReadOnlyDictionary<string, string> dataSource, DataSourceFormat dataFormat, IKustoIngestClient ingestClient, string configDatabaseName, string configTableName, string mappingName)
        {
            var sourceType = dataSource["sourceType"].ToLower();
            var sourceUri = dataSource["dataSourceUri"];
            WaitForUserToProceed($"Ingest '{sourceUri}' from '{sourceType}'");

            // Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
            // If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
            dataFormat = dataFormat == DataSourceFormat.json ? DataSourceFormat.multijson : dataFormat;

            // Tip: Kusto's C# SDK can ingest data from files, blobs and open streams.See the SDK's samples and the E2E tests in azure.kusto.ingest for additional references.
            switch (sourceType)
            {
                case "localfilesource":
                    await Utils.IngestAsync(ingestClient, configDatabaseName, configTableName, sourceUri, dataFormat, mappingName, true);
                    break;
                case "blobsource":
                    await Utils.IngestAsync(ingestClient, configDatabaseName, configTableName, sourceUri, dataFormat, mappingName);
                    break;
                default:
                    Utils.ErrorHandler($"Unknown source '{sourceType}' for file '{sourceUri}'");
                    break;
            }
        }

        /// <summary>
        /// Third and final phase - simple queries to validate the hopefully successful run of the script.  
        /// </summary>
        /// <param name="queryProvider">Client to run queries</param>
        /// <param name="configDatabaseName">DB Name</param>
        /// <param name="configTableName">Table Name</param>
        /// <param name="configIngestData">Flag noting whether any data was ingested by the script</param>
        private static async Task PostIngestionQueryingAsync(ICslQueryProvider queryProvider, string configDatabaseName, string configTableName, bool configIngestData)
        {
            var optionalPostIngestionPrompt = configIngestData ? "post-ingestion " : "";

            WaitForUserToProceed($"Get {optionalPostIngestionPrompt}row count for '{configDatabaseName}.{configTableName}':");
            await QueryExistingNumberOfRowsAsync(queryProvider, configDatabaseName, configTableName);

            WaitForUserToProceed($"Get sample (2 records) of {optionalPostIngestionPrompt}data:");
            await QueryFirstTwoRowsAsync(queryProvider, configDatabaseName, configTableName);
        }

        /// <summary>
        /// Handles UX on prompts and flow of program 
        /// </summary>
        /// <param name="promptMsg"> Prompt to display to user.</param>
        private static void WaitForUserToProceed(string promptMsg)
        {
            Console.WriteLine($"\nStep {Step}: {promptMsg}");
            Step++;
            if (_WaitForUser)
            {
                Console.WriteLine("Press any key to proceed with this operation...");
                Console.ReadLine();
            }
        }
    }
}