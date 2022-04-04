using System.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data.Exceptions;
using Kusto.Ingest;
using Newtonsoft.Json;

namespace QuickStart
{
    /// <summary>
    /// ConfigJson object -  represents a cluster and DataBase connection configuration file.
    /// </summary>
    public class ConfigJson
    {
        public bool UseExistingTable { get; set; }
        public string? DatabaseName { get; set; } = null;
        public string? TableName { get; set; } = null;
        public string? TableSchema { get; set; } = null;
        public string? KustoUrl { get; set; } = null;
        public string? IngestUrl { get; set; } = null;
        public List<Dictionary<string, string>>? Data = null;
        public bool AlterTable { get; set; }
        public bool QueryData { get; set; }
        public bool IngestData { get; set; }
    }


    /// <summary>
    /// 
    /// </summary>
    public class KustoSampleApp
    {
        // TODO (config - optional): Change the authentication method from "User Prompt" to any of the other options
        //  Some of the auth modes require additional environment variables to be set in order to work (see usage in generate_connection_string below)
        //  Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
        private const string
            AuthenticationMode = "UserPrompt"; // Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)

        // TODO (config - optional): Toggle to False to execute this script "unattended"
        private const bool WaitForUser = true;

        // TODO (config):
        // If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details
        // If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately
        private const string ConfigFileName = @"kusto_sample_config.json";
        private const int WaitForIngestSeconds = 20;

        private const string BatchingPolicy = "{ 'MaximumBatchingTimeSpan': '00:00:10', 'MaximumNumberOfItems': 500, " +
                                              "'MaximumRawDataSizeMB': 1024 }";

        private const string ErrorMsg = "Script failed with error: ";
        private const string ExceptionMsg = "Exception: ";
        private const string StartMsg = "Kusto sample app is starting...";
        private const string EndMsg = "Kusto sample app done";
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
            "Failed to alter the ingestion policy, which could be the result of insufficient permissions. The sample will still run, though ingestion will be delayed for up to 5 minutes.";

        private const string AuthenticationPrompt =
            "You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.";

        private const string ClientControlCmdError =
            "Client error while trying to execute control command '{0}' on database '{1}'";

        private const string ServiceControlCmdError =
            "Server error while trying to execute control command '{0}' on database '{1}'";

        private const string UnknownControlCmdError =
            "Unknown error while trying to execute control command '{0}' on database '{1}'";

        private const string AmountOfRecordsPrompt = "There are {0} records in the table";
        private const string ControlCmdResponse = "Response from executed control command '{0}':";
        private const string ControlCmdScope = "Python_SampleApp_ControlCommand";
        private const string QueryScope = "Python_SampleApp_Query";
        private static int _step = 1;


        /// <summary>
        /// Main engine - runs the actual script. 
        /// </summary>
        private void Start()
        {
            var config = LoadConfigs(ConfigFileName);

            if (AuthenticationMode == "UserPrompt")
                WaitForUserToProceed(AuthenticationPrompt);

            var kustoConnectionString = GenerateConnectionString(config?.KustoUrl, AuthenticationMode);
            var ingestConnectionString = GenerateConnectionString(config?.IngestUrl, AuthenticationMode);

            //Tip: Avoid creating a new Kusto/ingest client for each use.Instead,create the clients once and reuse them.
            var adminClient = KustoClientFactory.CreateCslAdminProvider(kustoConnectionString);
            var queryProvider = KustoClientFactory.CreateCslQueryProvider(kustoConnectionString);
            var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConnectionString);

            if (config is {UseExistingTable: true})
            {
                if (config is {AlterTable: true})
                    // Tip: Usually table was originally created with a schema appropriate for the data being ingested,
                    // so this wouldn't be needed.
                    // Learn More: For more information about altering table schemas, see:
                    // https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                    AlterMergeExistingTableToProvidedSchema(adminClient, config.DatabaseName, config.TableName,
                        config.TableSchema);

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
        /// Generates Kusto Connection String based on given Authentication Mode.
        /// </summary>
        /// <param name="clusterUrl"> Cluster to connect to.</param>
        /// <param name="authenticationMode">User Authentication Mode, 
        ///                                  Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)</param>
        /// <returns>A connection string to be used when creating a Client</returns>
        private KustoConnectionStringBuilder? GenerateConnectionString(string? clusterUrl, string authenticationMode)
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
        private KustoConnectionStringBuilder CreateManagedIdentityConnectionString(string? clusterUrl)
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
        /// Alter-merges the given existing table to provided schema.
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="configTableSchema">Table Schema</param>
        private void AlterMergeExistingTableToProvidedSchema(ICslAdminProvider adminClient, string? configDatabaseName,
            string? configTableName, string? configTableSchema)
        {
            WaitForUserToProceed(string.Format(AlterMergePrompt, configDatabaseName, configTableName));
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
        private void QueryExistingNumberOfRows(ICslQueryProvider queryClient, string? configDatabaseName,
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
        private void CreateNewTable(ICslAdminProvider adminClient, string? configDatabaseName, string? configTableName,
            string? configTableSchema)
        {
            WaitForUserToProceed(string.Format(CreateNewTablePrompt, configDatabaseName, configTableName));
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
        private void AlterBatchingPolicy(ICslAdminProvider adminClient, string? configDatabaseName,
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
        /// Executes Control Command using a privileged client
        /// </summary>
        /// <param name="adminClient">Privileged client to run Control Commands</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="command">The Control Command</param>
        /// <returns>True on success, false otherwise</returns>
        private bool ExecuteControlCommand(ICslAdminProvider adminClient, string? configDatabaseName, string command)
        {
            try
            {
                var clientRequestProperties = CreateClientRequestProperties(ControlCmdScope);
                using var result = adminClient
                    .ExecuteControlCommandAsync(configDatabaseName, command, clientRequestProperties)
                    .GetAwaiter().GetResult();

                // Tip: Actual implementations wouldn't generally print the response from a control command.
                // We print here to demonstrate what the response looks like.
                Console.WriteLine(string.Format(ControlCmdResponse, command));
                var schemaTable = result.GetSchemaTable();
                foreach (DataRow row in schemaTable.Rows)
                {
                    foreach (DataColumn column in schemaTable.Columns)
                        Console.WriteLine($"{column.ColumnName} = {row[column]}");
                }

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
                Console.WriteLine(string.Format(msg, command, configDatabaseName));
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
        private bool ExecuteQuery(ICslQueryProvider queryClient, string? configDatabaseName, string query)
        {
            try
            {
                var clientRequestProperties = CreateClientRequestProperties(QueryScope);
                using var result = queryClient.ExecuteQueryAsync(configDatabaseName, query, clientRequestProperties)
                    .GetAwaiter().GetResult();

                // Print how many records were read
                while (result.Read())
                    Console.WriteLine(string.Format(AmountOfRecordsPrompt, result.GetInt64(0)));

                // Move on to the next result set, SampleRecords
                result.NextResult();
                Console.WriteLine();
                while (result.Read())
                {
                    // Important note: For demonstration purposes we show how to read the data
                    // using the "bare bones" IDataReader interface. In a production environment
                    // one would normally use some ORM library to automatically map the data from
                    // IDataReader into a strongly-typed record type (e.g. Dapper.Net, AutoMapper, etc.)
                    var time = result.GetDateTime(0);
                    var type = result.GetString(1);
                    var state = result.GetString(2);
                    Console.WriteLine("{0}\t{1,-20}\t{2}", time, type, state);
                }

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
                Console.WriteLine(string.Format(msg, query, configDatabaseName));
            }

            return false;
        }


        /// <summary>
        /// Creates a fitting ClientRequestProperties object, to be used when executing control commands or queries.
        /// </summary>
        /// <param name="scope">Working scope</param>
        /// <param name="timeout">Requests default timeout</param>
        /// <returns>ClientRequestProperties object</returns>
        private ClientRequestProperties CreateClientRequestProperties(string scope, string? timeout = null)
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
        /// Handles UX on prompts and flow of program 
        /// </summary>
        /// <param name="promptMsg"> Prompt to display to user.</param>
        private void WaitForUserToProceed(string promptMsg)
        {
            Console.WriteLine(string.Format(UpcomingOperationPrompt, _step, promptMsg));
            _step++;
            if (WaitForUser)
            {
                Console.WriteLine(ContinueMsg);
                Console.ReadLine();
            }
        }


        /// <summary>
        /// Loads JSON configuration file, and sets the metadata in place. 
        /// </summary>
        /// <param name="configFilePath"> Configuration file path.</param>
        /// <returns>ConfigJson object, allowing access to the metadata fields.</returns>
        private ConfigJson? LoadConfigs(string configFilePath)
        {
            try
            {
                using var r = new StreamReader(configFilePath);
                var json = r.ReadToEnd();
                var config = JsonConvert.DeserializeObject<ConfigJson>(json);

                // TODO: can be implemented with Any?
                if (config.DatabaseName is null || config.TableName is null || config.TableSchema is null ||
                    config.KustoUrl is null || config.IngestUrl is null || config.Data is null)
                    ErrorHandler(string.Format(MissingFieldsError, configFilePath));

                return config;
            }

            catch (Exception ex)
            {
                ErrorHandler(string.Format(ConfigFileError, configFilePath), ex);
            }

            return null; // TODO: can it even reach here with the try catch block?
        }


        /// <summary>
        /// Error handling function. Will mention the appropriate error message (and the exception itself if exists),
        /// and will quit the program. 
        /// </summary>
        /// <param name="error">Appropriate error message received from calling function</param>
        /// <param name="e">Thrown exception</param>
        private void ErrorHandler(string error, Exception? e = null)
        {
            Console.WriteLine(ErrorMsg + error);
            if (e is not null)
                Console.WriteLine(ExceptionMsg + e);

            Environment.Exit(-1);
        }


        public static void Main()
        {
            Console.WriteLine(StartMsg);
            new KustoSampleApp().Start();
            Console.WriteLine(EndMsg);
        }
    }
}