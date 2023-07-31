using System;
using System.Linq;
using System.Threading.Tasks;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Exceptions;
using Kusto.Ingest;
using Kusto.Ingest.Exceptions;
using Kusto.Cloud.Platform.Data;
using ShellProgressBar;
using Newtonsoft.Json.Linq;

namespace QuickStart
{

    /// <summary>
    /// Util static class - Handles the communication with the API, and provides generic and simple "plug-n-play" functions to use in different programs.
    /// </summary>
    public static class Utils
    {
        /// <summary>
        /// Authentication module of Utils - in charge of authenticating the user with the system
        /// </summary>
        public static class Authentication
        {

            /// <summary>
            /// Generates Kusto Connection String based on given Authentication Mode.
            /// </summary>
            /// <param name="clusterUrl"> Cluster to connect to.</param>
            /// <param name="authenticationMode">User Authentication Mode, Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)</param>
            /// <param name="tenantId">Given tenant id</param>
            /// <returns>A connection string to be used when creating a Client</returns>
            public static KustoConnectionStringBuilder GenerateConnectionString(string clusterUrl, AuthenticationModeOptions authenticationMode, string tenantId)
            {
                // Learn More: For additional information on how to authorize users and apps in Kusto see:
                // https://docs.microsoft.com/azure/data-explorer/manage-database-permissions
                switch (authenticationMode)
                {
                    case AuthenticationModeOptions.UserPrompt:
                        // Prompt user for credentials
                        return new KustoConnectionStringBuilder(clusterUrl).WithAadUserPromptAuthentication();

                    case AuthenticationModeOptions.ManagedIdentity:
                        // Authenticate using a System-Assigned managed identity provided to an azure service, or using a User-Assigned managed identity.
                        // For more information, see https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
                        return CreateManagedIdentityConnectionString(clusterUrl);

                    case AuthenticationModeOptions.AppKey:
                        // Learn More: For information about how to procure an AAD Application,
                        // see: https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
                        // TODO (config - optional): App ID & tenant, and App Key to authenticate with
                        return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationKeyAuthentication(Environment.GetEnvironmentVariable("APP_ID"), Environment.GetEnvironmentVariable("APP_KEY"), Environment.GetEnvironmentVariable("APP_TENANT"));

                    case AuthenticationModeOptions.AppCertificate:
                        // Authenticate using a certificate file.
                        return CreateAppCertificateConnectionString(clusterUrl, tenantId);

                    default:
                        ErrorHandler($"Authentication mode '{authenticationMode}' is not supported");
                        return null;
                }
            }

            /// <summary>
            /// Generates Kusto Connection String based on 'ManagedIdentity' Authentication Mode.
            /// </summary>
            /// <param name="clusterUrl">Url of cluster to connect to</param>
            /// <returns>ManagedIdentity Kusto Connection String</returns>
            public static KustoConnectionStringBuilder CreateManagedIdentityConnectionString(string clusterUrl)
            {
                // Connect using the system - or user-assigned managed identity (Azure service only)
                // TODO (config - optional): Managed identity client ID if you are using a user-assigned managed identity
                var clientId = Environment.GetEnvironmentVariable("MANAGED_IDENTITY_CLIENT_ID");
                return clientId is null ? new KustoConnectionStringBuilder(clusterUrl).WithAadSystemManagedIdentity() : new KustoConnectionStringBuilder(clusterUrl).WithAadUserManagedIdentity(clientId);
            }

            /// <summary>
            /// Generates Kusto Connection String based on 'AppCertificate' Authentication Mode.
            /// </summary>
            /// <param name="clusterUrl">Url of cluster to connect to</param>
            /// <param name="tenantId">Given tenant id</param>
            /// <returns>AppCertificate Kusto Connection String</returns>
            public static KustoConnectionStringBuilder CreateAppCertificateConnectionString(string clusterUrl, string tenantId)
            {
                var appId = Environment.GetEnvironmentVariable("APP_ID");
                var subjectDistinguishedName = Environment.GetEnvironmentVariable("SUBJECT_DISTINGUISHED_NAME");
                var issuerDistinguishedName = Environment.GetEnvironmentVariable("ISSUER_DISTINGUISHED_NAME");

                return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationSubjectAndIssuerAuthentication(appId, subjectDistinguishedName, issuerDistinguishedName, tenantId);
            }

        }

        /// <summary>
        /// Queries module of Utils - in charge of querying the data - either with management queries, or data queries
        /// </summary>
        public static class Queries
        {
            private const string MgmtPrefix = ".";

            /// <summary>
            /// Creates a fitting ClientRequestProperties object, to be used when executing control commands or queries.
            /// </summary>
            /// <param name="scope">Working scope</param>
            /// <param name="timeout">Requests default timeout</param>
            /// <returns>ClientRequestProperties object</returns>
            public static ClientRequestProperties CreateClientRequestProperties(string scope, string timeout = null)
            {
                var clientRequestProperties = new ClientRequestProperties
                {
                    Application = "QuickStart.csproj",
                    // It is strongly recommended that each request has its own unique request identifier.
                    // This is mandatory for some scenarios (such as cancelling queries) and will make troubleshooting easier in others.
                    ClientRequestId = $"{scope};{Guid.NewGuid().ToString()}"
                };
                // Tip: Though uncommon, you can alter the request default command timeout using the below command, e.g. to set the timeout to 10 minutes, use "10m"
                if (timeout != null)
                {
                    clientRequestProperties.SetOption(ClientRequestProperties.OptionServerTimeout, timeout);
                }

                return clientRequestProperties;
            }

            /// <summary>
            /// Executes a Command using a premade client
            /// </summary>
            /// <param name="client">Premade client to run Commands. can be either an adminClient or queryClient</param>
            /// <param name="configDatabaseName">DB name</param>
            /// <param name="command">The Command to execute</param>
            /// <returns>True on success, false otherwise</returns>
            public static async Task ExecuteAsync(IDisposable client, string configDatabaseName, string command)
            {
                try
                {
                    JObject[] result;
                    if (command.StartsWith(MgmtPrefix)) // All control commands start with a specific prefix (usually '.') - and require a different client and scope to execute with.
                    {
                        var clientRequestProperties = CreateClientRequestProperties("CS_SampleApp_ControlCommand");
                        ICslAdminProvider adminClient = (ICslAdminProvider)client;
                        var disposableResult = (await adminClient.ExecuteControlCommandAsync(configDatabaseName, command, clientRequestProperties));
                        result = disposableResult.ToJObjects().ToArray();
                        disposableResult.Dispose();
                    }
                    else
                    {
                        var clientRequestProperties = CreateClientRequestProperties("CS_SampleApp_Query");
                        ICslQueryProvider queryClient = (ICslQueryProvider)client;
                        var disposableResult = (await queryClient.ExecuteQueryAsync(configDatabaseName, command, clientRequestProperties));
                        result = disposableResult.ToJObjects().ToArray();
                        disposableResult.Dispose();
                    }

                    // Tip: Actual implementations wouldn't generally print the response from a control command or a query .We print here to demonstrate what a sample of the response looks like.
                    // Moreover, there are some built-in classes for control commands under the Kusto.Data namespace -for example, Kusto.Data.TablesShowCommandResult maps to the result of the ".show tables" commands
                    Console.WriteLine($"Response from executed command '{command}':\n--------------------");
                    var firstRow = result[0];
                    foreach (var item in firstRow.Properties())
                    {
                        Console.WriteLine(item);
                    }
                }
                catch (KustoClientException ex)
                {
                    ErrorHandler($"Client error while trying to execute command '{command}' on database '{configDatabaseName}'", ex);
                }
                catch (Kusto.Data.Exceptions.KustoServiceException ex)
                {
                    ErrorHandler($"Server error while trying to execute command '{command}' on database '{configDatabaseName}'", ex);
                }
                catch (Exception ex)
                {
                    ErrorHandler($"Unknown error while trying to execute command '{command}' on database '{configDatabaseName}'", ex);
                }
            }

        }
        
        /// <summary>
        /// Ingestion module of Utils - in charge of ingesting the given data - based on the configuration file.
        /// </summary>
        public static class Ingestion
        {

            /// <summary>
            /// Creates a fitting KustoIngestionProperties object, to be used when executing ingestion commands.
            /// </summary>
            /// <param name="configDatabaseName">DB name</param>
            /// <param name="configTableName">Table name</param>
            /// <param name="dataFormat">Given data format</param>
            /// <param name="mappingName">Desired mapping name</param>
            /// <param name="ignoreFirstRecord">Flag noting whether to ignore the first record in the table</param>
            /// <returns>KustoIngestionProperties object</returns>
            public static KustoIngestionProperties CreateIngestionProperties(string configDatabaseName, string configTableName, DataSourceFormat dataFormat, string mappingName, bool ignoreFirstRecord)
            {
                var kustoIngestionProperties = new KustoIngestionProperties()
                {
                    DatabaseName = configDatabaseName,
                    TableName = configTableName,
                    IngestionMapping = new IngestionMapping() { IngestionMappingReference = mappingName },
                    IgnoreFirstRecord = ignoreFirstRecord,
                    Format = dataFormat
                };

                return kustoIngestionProperties;
            }

            /// <summary>
            /// Ingest Data from a given file path.
            /// </summary>
            /// <param name="ingestClient">Client to ingest data</param>
            /// <param name="configDatabaseName">DB name</param>
            /// <param name="configTableName">Table name</param>
            /// <param name="uri">Uri to ingest from</param>
            /// <param name="dataFormat">Given data format</param>
            /// <param name="mappingName">Desired mapping name</param>
            /// <param name="ignoreFirstRecord">Flag noting whether to ignore the first record in the table</param>
            /// <param name="isFile">Flag indicating whether the uri is of a file or not.</param>
            public static async Task IngestAsync(IKustoIngestClient ingestClient, string configDatabaseName, string configTableName, string uri, DataSourceFormat dataFormat, string mappingName, bool ignoreFirstRecord, bool isFile = false)
            {
                var ingestionProperties = CreateIngestionProperties(configDatabaseName, configTableName, dataFormat, mappingName, ignoreFirstRecord);
                // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor instead of the default below of 0. 
                // Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
                // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere.
                var sourceOptions = new StorageSourceOptions
                {
                    Size = 0,
                    SourceId = Guid.NewGuid()
                };

                // Tip 3: To instruct the client to ingest a *file* (and not another source type), we can either provide it with a path as the sourceUri, or use the IsLocalFileSystem = true flag.
                if (isFile)
                {
                    sourceOptions.IsLocalFileSystem = true;
                }

                try
                {
                    await ingestClient.IngestFromStorageAsync(uri, ingestionProperties, sourceOptions);
                }
                catch (IngestClientException ex)
                {
                    ErrorHandler($"Client error while trying to ingest from '{uri}'", ex);
                }
                catch (Kusto.Ingest.Exceptions.KustoServiceException ex)
                {
                    ErrorHandler($"Server error while trying to ingest from '{uri}'", ex);
                }
                catch (Exception ex)
                {
                    ErrorHandler($"Unknown error while trying to ingest from '{uri}'", ex);
                }
            }

            /// <summary>
            /// Halts the program for WaitForIngestSeconds, allowing the queued ingestion process to complete.
            /// </summary>
            public static async Task WaitForIngestionToCompleteAsync(int WaitForIngestSeconds)
            {
                Console.WriteLine($"Sleeping {WaitForIngestSeconds} seconds for queued ingestion to complete. Note: This may take longer depending on the file size and ingestion batching policy.");
                Console.WriteLine();
                Console.WriteLine();

                var options = new ProgressBarOptions
                {
                    ProgressCharacter = '#',
                    ProgressBarOnBottom = true,
                    ForegroundColor = ConsoleColor.White,
                };
                using (var pbar = new ProgressBar(WaitForIngestSeconds * 2, "", options))
                {
                    for (var i = WaitForIngestSeconds * 2; i >= 0; i--)
                    {
                        pbar.Tick();
                        await Task.Delay(500);
                    }
                }
            }

        }

        /// <summary>
        /// Error handling function. Will mention the appropriate error message (and the exception itself if exists), and will quit the program. 
        /// </summary>
        /// <param name="error">Appropriate error message received from calling function</param>
        /// <param name="e">Thrown exception</param>
        public static void ErrorHandler(string error, Exception e = null)
        {
            Console.WriteLine($"Script failed with error: {error}");
            if (e != null)
            {
                Console.WriteLine($"Exception: {e}");
            }

            Environment.Exit(-1);
        }
    }
}