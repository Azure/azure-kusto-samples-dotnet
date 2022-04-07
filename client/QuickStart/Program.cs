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
    /// ConfigJson object -  represents a cluster and DataBase connection configuration file.
    /// </summary>
    public class ConfigJson
    {
        public bool? UseExistingTable = null;
        public string? DatabaseName = null;
        public string? TableName = null;
        public string? TableSchema = null;
        public string? KustoUri = null;
        public string? IngestUri = null;
        public List<Dictionary<string, string?>>? Data = null;
        public bool? AlterTable = null;
        public bool? QueryData = null;
        public bool? IngestData = null;
    }


    /// <summary>
    /// The quick start application is a self-contained and runnable example script that demonstrates authenticating,
    /// connecting to, administering, ingesting data into and querying Azure Data Explorer using the azure-kusto C# SDK.
    /// You can use it as a baseline to write your own first kusto client application, altering the code as you go,
    /// or copy code sections out of it into your app.
    /// Tip: The app includes comments with tips on recommendations, coding best practices, links to reference materials
    /// and recommended TO DO changes when adapting the code to your needs.
    /// </summary>
    public static class KustoSampleApp
    {
        //########################### Constants Start ###########################// 

        // TODO (config - optional): Change the authentication method from "User Prompt" to any of the other options
        //  Some of the auth modes require additional environment variables to be set in order to work
        // (see usage in generate_connection_string below).
        //  Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
        private const string
            AuthenticationMode = "UserPrompt"; // Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)

        // TODO (config - optional): Toggle to False to execute this script "unattended"
        private const bool WaitForUser = true;

        // TODO (config):
        // If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated
        // with your cluster's details.
        // If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL
        // and database fields appropriately.
        private const string ConfigFileName = @"kusto_sample_config.json";
        private const int WaitForIngestSeconds = 20;

        private const string BatchingPolicy = "{ 'MaximumBatchingTimeSpan': '00:00:10', 'MaximumNumberOfItems': 500, " +
                                              "'MaximumRawDataSizeMB': 1024 }";

        private const string ErrorMsg = "Script failed with error: ";
        private const string ExceptionMsg = "Exception: ";
        private const string StartMsg = "Kusto sample app is starting...";
        private const string EndMsg = "\nKusto sample app done";
        private const string MissingFieldsError = "File '{0}' is missing required fields";
        private const string ConfigFileError = "Couldn't read config file: '{0}'";
        private const string UpcomingOperationPrompt = "\nStep {0}: {1}";
        private const string ContinueMsg = "Press any key to proceed with this operation...";
        private const string ManagedIdEnvVar = "MANAGED_IDENTITY_CLIENT_ID";
        private const string AppIdEnvVar = "APP_ID";
        private const string AppKeyEnvVar = "APP_KEY";
        private const string AppTenantEnvVar = "APP_TENANT";
        private const string InvalidAuthenticationModeError = "Authentication mode '{0}' is not supported";

        private const string AlterMergePrompt =
            "Alter-merge existing table '{0}.{1}' to align with the provided schema";

        private const string AlterMergeCmd = ".alter-merge table {0} {1}";
        private const string AlterMergeError = "Failed to alter table using command '{0}'";
        private const string ExecuteQueryError = "Failed to execute query: '{0}'";
        private const string ExistingNumberOfRowsPrompt = "Get existing row count in '{0}.{1}'";
        private const string ExistingNumberOfRowsQuery = "{0} | count";
        private const string CreateNewTablePrompt = "$Create table '{0}.{1}'";
        private const string CreateNewTableCmd = ".create table {0} {1}";
        private const string CreateNewTableError = "Failed to create table or validate it exists using command '{0}'";
        private const string AlterBatchingPrompt = "Alter the batching policy for table '{0}.{1}'";
        private const string AlterBatchingCmd = ".alter table {0} policy ingestionbatching @'{1}'";

        private const string AlterBatchingError =
            "Failed to alter the ingestion policy, which could be the result of insufficient permissions. The sample " +
            "will still run, though ingestion will be delayed for up to 5 minutes.";

        private const string AuthenticationPrompt =
            "You will be prompted *twice* for credentials during this script. Please return to the console after " +
            "authenticating.";

        private const string ClientControlCmdError =
            "Client error while trying to execute control command '{0}' on database '{1}'";

        private const string ServiceControlCmdError =
            "Server error while trying to execute control command '{0}' on database '{1}'";

        private const string UnknownControlCmdError =
            "Unknown error while trying to execute control command '{0}' on database '{1}'";

        private const string ControlCmdResponse = "Response from executed control command '{0}':\n--------------------";
        private const string QueryResponse = "Response from executed query '{0}':\n--------------------";
        private const string ControlCmdScope = "Python_SampleApp_ControlCommand";
        private const string QueryScope = "Python_SampleApp_Query";
        private const string IngestionMappingPrompt = "Create a '{0}' mapping reference named '{1}'";
        private const string DefaultMappingName = "DefaultQuickstartMapping";
        private const string IngestionMappingCommand = ".create-or-alter table {0} ingestion {1} mapping '{2}' '{3}'";

        private const string IngestionMappingError =
            "Failed to create a '{0}' mapping reference named '{1}'. Skipping this ingestion.";

        private const string IngestUriPrompt = "Ingest '{0}' from '{1}'";
        private const string IngestUriError = "Unknown source '{0}' for file '{1}'";

        private const string IngestionSleepPrompt =
            "Sleeping {0} seconds for queued ingestion to complete. Note: This may take longer depending on the file" +
            " size and ingestion batching policy.";

        private const string PostIngestionPrompt = "post-ingestion ";
        private const string ValidationRowCountPrompt = "Get {0}row count for '{1}.{2}':";
        private const string ValidationSamplePrompt = "Get sample (2 records) of {0}data:";
        private const string SampleQuery = "{0} | take 2";
        private static int _step = 1;

        //########################### Constants End ###########################// 


        public static async Task Main()
        {
            Console.WriteLine(StartMsg);
            await Start();
            Console.WriteLine(EndMsg);
        }


        /// <summary>
        /// Main engine - runs the actual script. 
        /// </summary>
        private static async Task Start()
        {
            var config = LoadConfigs(ConfigFileName);

            if (AuthenticationMode == "UserPrompt")
                WaitForUserToProceed(AuthenticationPrompt);

            var kustoConnectionString = GenerateConnectionString(config?.KustoUri, AuthenticationMode);
            var ingestConnectionString = GenerateConnectionString(config?.IngestUri, AuthenticationMode);

            //Tip: Avoid creating a new Kusto/ingest client for each use.Instead,create the clients once and reuse them.
            var adminClient = KustoClientFactory.CreateCslAdminProvider(kustoConnectionString);
            var queryProvider = KustoClientFactory.CreateCslQueryProvider(kustoConnectionString);
            var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConnectionString);

            BasicTableCommands(config, adminClient, queryProvider);
            await IngestionHandler(config, adminClient, ingestClient);
            if (config is {QueryData: true})
                ExecuteValidationQueries(queryProvider, config.DatabaseName, config.TableName, config.IngestData);
        }


        /// <summary>
        /// Loads JSON configuration file, and sets the metadata in place. 
        /// </summary>
        /// <param name="configFilePath"> Configuration file path.</param>
        /// <returns>ConfigJson object, allowing access to the metadata fields.</returns>
        private static ConfigJson? LoadConfigs(string configFilePath)
        {
            try
            {
                using var r = new StreamReader(configFilePath);
                var json = r.ReadToEnd();
                var config = JsonConvert.DeserializeObject<ConfigJson>(json);
                
                if (config.DatabaseName is null || config.TableName is null || config.TableSchema is null ||
                    config.KustoUri is null || config.IngestUri is null || config.Data is null)
                    ErrorHandler(string.Format(MissingFieldsError, configFilePath));

                return config;
            }

            catch (Exception ex)
            {
                ErrorHandler(string.Format(ConfigFileError, configFilePath), ex);
            }

            return null;
        }


        /// <summary>
        /// Generates Kusto Connection String based on given Authentication Mode.
        /// </summary>
        /// <param name="clusterUrl"> Cluster to connect to.</param>
        /// <param name="authenticationMode">User Authentication Mode, 
        ///                                  Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)</param>
        /// <returns>A connection string to be used when creating a Client</returns>
        private static KustoConnectionStringBuilder? GenerateConnectionString(string? clusterUrl,
            string authenticationMode)
        {
            //Learn More: For additional information on how to authorize users and apps in Kusto see:
            //https://docs.microsoft.com/azure/data-explorer/manage-database-permissions
            switch (authenticationMode)
            {
                case "UserPrompt":
                    // Prompt user for credentials
                    return new KustoConnectionStringBuilder(clusterUrl).WithAadUserPromptAuthentication();

                case "ManagedIdentity":
                    // TODO: add documentation
                    return CreateManagedIdentityConnectionString(clusterUrl);

                case "AppKey":
                    // Learn More: For information about how to procure an AAD Application, see:
                    // https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
                    // TODO (config - optional): App ID & tenant, and App Key to authenticate with
                    return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationKeyAuthentication(
                        Environment.GetEnvironmentVariable(AppIdEnvVar),
                        Environment.GetEnvironmentVariable(AppKeyEnvVar),
                        Environment.GetEnvironmentVariable(AppTenantEnvVar));

                case "AppCertificate":
                // TODO: add documentation
                //return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationCertificateAuthentication(); // TODO: Asaf

                default:
                    ErrorHandler(string.Format(InvalidAuthenticationModeError, authenticationMode));
                    return null;
            }
        }


        /// <summary>
        /// Generates Kusto Connection String based on 'ManagedIdentity' Authentication Mode.
        /// </summary>
        /// <param name="clusterUrl"></param>
        /// <returns>ManagedIdentity Kusto Connection String</returns>
        private static KustoConnectionStringBuilder CreateManagedIdentityConnectionString(string? clusterUrl)
        {
            //Connect using the system - or user-assigned managed identity (Azure service only)
            // TODO (config - optional): Managed identity client ID if you are using a user-assigned managed identity
            var clientId = Environment.GetEnvironmentVariable(ManagedIdEnvVar);
            if (clientId is null)
                return new KustoConnectionStringBuilder(clusterUrl)
                    .WithAadSystemManagedIdentity();

            return new KustoConnectionStringBuilder(clusterUrl)
                .WithAadUserManagedIdentity(clientId);
        }

        
        /// <summary>
        /// Basic Table Commands - including AlterMerge Existing Table and Create Table control commands, and
        /// Existing Number Of Rows query.
        /// </summary>
        /// <param name="config">ConfigJson object</param>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="queryProvider">Client to run queries</param>
        private static void BasicTableCommands(ConfigJson? config, ICslAdminProvider adminClient,
            ICslQueryProvider queryProvider)
        {
            if (config is {UseExistingTable: true})
            {
                if (config is {AlterTable: true})
                    // Tip: Usually table was originally created with a schema appropriate for the data being ingested,
                    // so this wouldn't be needed.
                    // Learn More: For more information about altering table schemas, see:
                    // https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                    AlterMergeExistingTableToProvidedSchema(adminClient, config.DatabaseName,
                        config.TableName, config.TableSchema);

                if (config is {QueryData: true})
                    // Learn More: For more information about Kusto Query Language (KQL), see:
                    // https://docs.microsoft.com/azure/data-explorer/write-queries
                    QueryExistingNumberOfRows(queryProvider, config.DatabaseName, config.TableName);
            }
            else
                //Tip: This is generally a one-time configuration
                //Learn More: For more information about creating tables, see:
                //https://docs.microsoft.com/azure/data-explorer/one-click-table
                CreateNewTable(adminClient, config?.DatabaseName, config?.TableName, config?.TableSchema);
        }

        
        /// <summary>
        /// Entire ingestion process.
        /// </summary>
        /// <param name="config">ConfigJson object</param>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="ingestClient">Client to ingest data</param>
        private static async Task IngestionHandler(ConfigJson? config, ICslAdminProvider adminClient,
            IKustoIngestClient ingestClient)
        {
            if (config is not {IngestData: true}) return;
            foreach (var file in config.Data!)
            {
                var dataFormat = (DataSourceFormat) Enum.Parse(typeof(DataSourceFormat),
                    file["format"]!.ToLower());
                var mappingName = file["mappingName"];

                // Tip: This is generally a one-time configuration.
                // Learn More: For more information about providing inline mappings and mapping references,
                // see: https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
                if (!CreateIngestionMappings(
                        bool.Parse(char.ToUpper(file["useExistingMapping"]![0]) +
                                   file["useExistingMapping"]?[1..]), adminClient, config.DatabaseName,
                        config.TableName, mappingName, file["mappingValue"], dataFormat))
                    continue;

                // Learn More: For more information about ingesting data to Kusto in Python,
                // see: https://docs.microsoft.com/azure/data-explorer/python-ingest-data
                await Ingest(file, dataFormat, ingestClient, config.DatabaseName, config.TableName, mappingName);
            }

            WaitForIngestionToComplete();
        }

        
        /// <summary>
        /// Alter-merges the given existing table to provided schema.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="configTableSchema">Table Schema</param>
        private static void AlterMergeExistingTableToProvidedSchema(ICslAdminProvider adminClient,
            string? configDatabaseName, string? configTableName, string? configTableSchema)
        {
            WaitForUserToProceed(string.Format(AlterMergePrompt, configDatabaseName, configTableName));

            // You can also use the CslCommandGenerator class to build commands:
            // string command = CslCommandGenerator.GenerateTableAlterMergeCommand();
            var command = string.Format(AlterMergeCmd, configTableName, configTableSchema);

            if (!ExecuteControlCommand(adminClient, configDatabaseName, command))
                ErrorHandler(string.Format(AlterMergeError, command));
        }


        /// <summary>
        /// Queries the data on the existing number of rows.
        /// </summary>
        /// <param name="queryClient">Client to run queries</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        private static void QueryExistingNumberOfRows(ICslQueryProvider queryClient, string? configDatabaseName,
            string? configTableName)
        {
            WaitForUserToProceed(string.Format(ExistingNumberOfRowsPrompt, configDatabaseName, configTableName));
            var query = string.Format(ExistingNumberOfRowsQuery, configTableName);
            if (!ExecuteQuery(queryClient, configDatabaseName, query))
                ErrorHandler(string.Format(ExecuteQueryError, query));
        }


        /// <summary>
        /// Creates a new table.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="configTableSchema">Table Schema</param>
        private static void CreateNewTable(ICslAdminProvider adminClient, string? configDatabaseName,
            string? configTableName,
            string? configTableSchema)
        {
            WaitForUserToProceed(string.Format(CreateNewTablePrompt, configDatabaseName, configTableName));

            // You can also use the CslCommandGenerator class to build commands:
            // string command = CslCommandGenerator.GenerateTableCreateCommand();
            var command = string.Format(CreateNewTableCmd, configTableName, configTableSchema);

            if (!ExecuteControlCommand(adminClient, configDatabaseName, command))
                ErrorHandler(string.Format(CreateNewTableError, command));

            // Learn More:
            // Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of
            // the following conditions are met:
            //   1) More than 1,000 files were queued for ingestion for the same table by the same user
            //   2) More than 1GB of data was queued for ingestion for the same table by the same user
            //   3) More than 5 minutes have passed since the first file was queued for ingestion for the same table
            //      by the same user
            //  For more information about customizing the ingestion batching policy, see:
            // https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy

            // TODO: Change if needed.
            // Disabled to prevent an existing batching policy from being unintentionally changed
            if (false)
                AlterBatchingPolicy(adminClient, configDatabaseName, configTableName);
        }


        /// <summary>
        /// Alters the batching policy based on BatchingPolicy const.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        private static void AlterBatchingPolicy(ICslAdminProvider adminClient, string? configDatabaseName,
            string? configTableName)
        {
            // Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during
            // development and in this sample app, we opt to modify the default ingestion policy to ingest data after
            // at most 10 seconds.
            // Tip 2: This is generally a one-time configuration.
            // Tip 3: You can also skip the batching for some files using the Flush-Immediately property, though this
            // option should be used with care as it is inefficient.

            WaitForUserToProceed(string.Format(AlterBatchingPrompt, configDatabaseName, configTableName));
            var command = string.Format(AlterBatchingCmd, configTableName, BatchingPolicy);
            if (!ExecuteControlCommand(adminClient, configDatabaseName, command))
                Console.WriteLine(AlterBatchingError);
        }


        /// <summary>
        /// Creates a fitting ClientRequestProperties object, to be used when executing control commands or queries.
        /// </summary>
        /// <param name="scope">Working scope</param>
        /// <param name="timeout">Requests default timeout</param>
        /// <returns>ClientRequestProperties object</returns>
        private static ClientRequestProperties CreateClientRequestProperties(string scope, string? timeout = null)
        {
            var clientRequestProperties = new ClientRequestProperties
            {
                Application = "QuickStart.csproj",
                // It is strongly recommended that each request has its own unique
                // request identifier. This is mandatory for some scenarios (such as cancelling queries)
                // and will make troubleshooting easier in others.
                ClientRequestId = $"{scope};{Guid.NewGuid().ToString()}"
            };
            // Tip: Though uncommon, you can alter the request default command timeout using the below command,
            // e.g. to set the timeout to 10 minutes, use "10m"
            if (timeout is not null)
                clientRequestProperties.SetOption(ClientRequestProperties.OptionServerTimeout, timeout);

            return clientRequestProperties;
        }


        /// <summary>
        /// Executes Control Command using a privileged client
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="command">The Control Command</param>
        /// <returns>True on success, false otherwise</returns>
        private static bool ExecuteControlCommand(ICslAdminProvider adminClient, string? configDatabaseName,
            string command)
        {
            try
            {
                var clientRequestProperties = CreateClientRequestProperties(ControlCmdScope);
                var result = adminClient
                    .ExecuteControlCommandAsync(configDatabaseName, command, clientRequestProperties)
                    .GetAwaiter().GetResult().ToJObjects().ToArray(); // In real code use await in async method

                // Tip: Actual implementations wouldn't generally print the response from a control command.
                // We print here to demonstrate what a sample of the response looks like.
                // Moreover, there are some built-in classes for control commands under the Kusto.Data namespace -
                // for example, Kusto.Data.TablesShowCommandResult maps to the result of the ".show tables" commands
                Console.WriteLine(ControlCmdResponse, command);
                var firstRow = result[0];
                foreach (var item in firstRow.Properties())
                    Console.WriteLine(item);

                return true;
            }

            catch (Exception ex)
            {
                var err = ex.GetType().ToString();
                var msg = err switch
                {
                    "KustoClientException" => ClientControlCmdError,
                    "KustoServiceException" => ServiceControlCmdError,
                    _ => UnknownControlCmdError
                };
                Console.WriteLine(msg, command, configDatabaseName);
            }

            return false;
        }


        /// <summary>
        /// Executes a query using a query client
        /// </summary>
        /// <param name="queryClient">Client to run queries</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="query">The query</param>
        /// <returns>True on success, false otherwise</returns>
        private static bool ExecuteQuery(ICslQueryProvider queryClient, string? configDatabaseName, string query)
        {
            try
            {
                var clientRequestProperties = CreateClientRequestProperties(QueryScope);
                var result = queryClient.ExecuteQueryAsync(configDatabaseName, query, clientRequestProperties)
                    .GetAwaiter().GetResult().ToJObjects().ToArray(); // In real code use await in async method

                // Tip: Actual implementations wouldn't generally print the response from a query.
                // We print here to demonstrate what a sample of the response looks like.
                Console.WriteLine(QueryResponse, query);
                var firstRow = result[0];
                foreach (var item in firstRow.Properties())
                    Console.WriteLine(item);

                return true;
            }

            catch (Exception ex)
            {
                var err = ex.GetType().ToString();
                var msg = err switch
                {
                    "KustoClientException" => ClientControlCmdError,
                    "KustoServiceException" => ServiceControlCmdError,
                    _ => UnknownControlCmdError
                };
                Console.WriteLine(msg, query, configDatabaseName);
            }

            return false;
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
        private static bool CreateIngestionMappings(bool useExistingMapping, ICslAdminProvider adminClient,
            string? configDatabaseName, string? configTableName, string? mappingName, string? mappingValue,
            DataSourceFormat dataFormat)
        {
            if (useExistingMapping || mappingValue is null)
                return true;

            var ingestionMappingKind = dataFormat.ToIngestionMappingKind().ToString().ToLower();
            WaitForUserToProceed(string.Format(IngestionMappingPrompt, ingestionMappingKind, mappingName));

            mappingName ??= DefaultMappingName + Guid.NewGuid().ToString()[..5];
            var mappingCommand = string.Format(IngestionMappingCommand, configTableName, ingestionMappingKind,
                mappingName, mappingValue);

            if (!ExecuteControlCommand(adminClient, configDatabaseName, mappingCommand))
                ErrorHandler(string.Format(IngestionMappingError, ingestionMappingKind, mappingName));

            return true;
        }


        /// <summary>
        /// Creates a fitting KustoIngestionProperties object, to be used when executing ingestion commands.
        /// </summary>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="mappingName">Desired mapping name</param>
        /// <returns>KustoIngestionProperties object</returns>
        private static KustoIngestionProperties CreateIngestionProperties(string? configDatabaseName,
            string? configTableName, DataSourceFormat dataFormat, string? mappingName)
        {
            var kustoIngestionProperties = new KustoIngestionProperties()
            {
                DatabaseName = configDatabaseName,
                TableName = configTableName,
                IngestionMapping = new IngestionMapping() {IngestionMappingReference = mappingName},
                Format = dataFormat
            };

            return kustoIngestionProperties;
        }


        /// <summary>
        /// Ingest data from given source.
        /// </summary>
        /// <param name="file">Given data source</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="ingestClient">Client to ingest data</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="mappingName">Desired mapping name</param>
        private static async Task Ingest(Dictionary<string, string?> file, DataSourceFormat dataFormat,
            IKustoIngestClient ingestClient, string? configDatabaseName, string? configTableName, string? mappingName)
        {
            var sourceType = file["sourceType"]?.ToLower();
            var sourceUri = file["dataSourceUri"];
            WaitForUserToProceed(string.Format(IngestUriPrompt, sourceUri, sourceType));

            // Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even
            // if the file only contains one line. If the json contains whitespace formatting, use SINGLEJSON.
            // In this case, only one data row json object is allowed per file.
            dataFormat = dataFormat == DataSourceFormat.json ? DataSourceFormat.multijson : dataFormat;

            // Tip: Kusto's C# SDK can ingest data from files, blobs and open streams.
            // See the SDK's samples and the E2E tests in azure.kusto.ingest for additional references.

            switch (sourceType)
            {
                case "localfilesource":
                    await IngestFromFile(ingestClient, configDatabaseName, configTableName, sourceUri, dataFormat,
                        mappingName);
                    break;
                case "blobsource":
                    await IngestFromBlob(ingestClient, configDatabaseName, configTableName, sourceUri, dataFormat,
                        mappingName);
                    break;
                default:
                    ErrorHandler(string.Format(IngestUriError, sourceType, sourceUri));
                    break;
            }
        }


        /// <summary>
        /// Ingest Data from a given file path.
        /// </summary>
        /// <param name="ingestClient">Client to ingest data</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="sourceUri">File path</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="mappingName">Desired mapping name</param>
        private static async Task IngestFromFile(IKustoIngestClient ingestClient, string? configDatabaseName,
            string? configTableName, string? sourceUri, DataSourceFormat dataFormat, string? mappingName)
        {
            var ingestionProperties =
                CreateIngestionProperties(configDatabaseName, configTableName, dataFormat, mappingName);

            // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the
            // file descriptor instead of the default below of 0. Otherwise, the service will determine the file size,
            // requiring an additional s2s call, and may not be accurate for compressed files.
            // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID
            // and log it somewhere.
            // Tip 3: To instruct the client to ingest a file (and not another source type), we can either provide it 
            // with a path as the sourceUri, or use the IsLocalFileSystem = true flag.
            var sourceOptions = new StorageSourceOptions()
            {
                Size = 0,
                SourceId = Guid.NewGuid(),
                IsLocalFileSystem = true
            };

            await ingestClient.IngestFromStorageAsync(sourceUri, ingestionProperties, sourceOptions);
        }


        /// <summary>
        /// Ingest Data from a Blob.
        /// </summary>
        /// <param name="ingestClient">Client to ingest data</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="sourceUri">Blob Uri</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="mappingName">Desired mapping name</param>
        private static async Task IngestFromBlob(IKustoIngestClient ingestClient, string? configDatabaseName,
            string? configTableName, string? sourceUri, DataSourceFormat dataFormat, string? mappingName)
        {
            var ingestionProperties =
                CreateIngestionProperties(configDatabaseName, configTableName, dataFormat, mappingName);

            // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the
            // file descriptor instead of the default below of 0. Otherwise, the service will determine the file size,
            // requiring an additional s2s call, and may not be accurate for compressed files.
            // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID
            // and log it somewhere
            var sourceOptions = new StorageSourceOptions() {Size = 0, SourceId = Guid.NewGuid()};
            await ingestClient.IngestFromStorageAsync(sourceUri, ingestionProperties, sourceOptions);
        }


        /// <summary>
        /// Halts the program for WaitForIngestSeconds, allowing the queued ingestion process to complete.
        /// </summary>
        private static void WaitForIngestionToComplete()
        {
            Console.WriteLine(IngestionSleepPrompt, WaitForIngestSeconds);
            Console.WriteLine();
            Console.WriteLine();

            var options = new ProgressBarOptions
            {
                ProgressCharacter = '#',
                ProgressBarOnBottom = true,
                ForegroundColor = ConsoleColor.White,
            };
            using var pbar = new ProgressBar(WaitForIngestSeconds * 2, "", options);
            for (var i = WaitForIngestSeconds * 2; i >= 0; i--)
            {
                pbar.Tick();
                Thread.Sleep(500);
            }
        }


        /// <summary>
        /// End-Of-Script simple queries, to validate the hopefully successful run of the script.  
        /// </summary>
        /// <param name="queryProvider">Client to run queries</param>
        /// <param name="configDatabaseName">DB Name</param>
        /// <param name="configTableName">Table Name</param>
        /// <param name="configIngestData">Flag noting whether any data was ingested by the script</param>
        private static void ExecuteValidationQueries(ICslQueryProvider queryProvider, string? configDatabaseName,
            string? configTableName, bool? configIngestData)
        {
            var optionalPostIngestionPrompt = configIngestData == true ? PostIngestionPrompt : "";
            WaitForUserToProceed(string.Format(ValidationRowCountPrompt, optionalPostIngestionPrompt,
                configDatabaseName, configTableName));

            var rowQuery = string.Format(ExistingNumberOfRowsQuery, configTableName);
            if (!ExecuteQuery(queryProvider, configDatabaseName, rowQuery))
                ErrorHandler(string.Format(ExecuteQueryError, rowQuery));

            WaitForUserToProceed(string.Format(ValidationSamplePrompt, optionalPostIngestionPrompt));

            var sampleQuery = string.Format(SampleQuery, configTableName);
            if (!ExecuteQuery(queryProvider, configDatabaseName, sampleQuery))
                ErrorHandler(string.Format(ExecuteQueryError, sampleQuery));
        }


        /// <summary>
        /// Handles UX on prompts and flow of program 
        /// </summary>
        /// <param name="promptMsg"> Prompt to display to user.</param>
        private static void WaitForUserToProceed(string promptMsg)
        {
            Console.WriteLine(UpcomingOperationPrompt, _step, promptMsg);
            _step++;
            if (WaitForUser)
            {
                Console.WriteLine(ContinueMsg);
                Console.ReadLine();
            }
        }


        /// <summary>
        /// Error handling function. Will mention the appropriate error message (and the exception itself if exists),
        /// and will quit the program. 
        /// </summary>
        /// <param name="error">Appropriate error message received from calling function</param>
        /// <param name="e">Thrown exception</param>
        private static void ErrorHandler(string error, Exception? e = null)
        {
            Console.WriteLine(ErrorMsg + error);
            if (e is not null)
                Console.WriteLine(ExceptionMsg + e);

            Environment.Exit(-1);
        }
    }
}