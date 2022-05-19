using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using System.Security.Cryptography.X509Certificates;
using ShellProgressBar;

namespace QuickStart
{
    /// <summary>
    /// ConfigJson object - represents a cluster and DataBase connection configuration file.
    /// </summary>
    public class ConfigJson
    {
        public bool UseExistingTable { get; set; }
        public string DatabaseName { get; set; }
        public string TableName { get; set; }
        public string TableSchema { get; set; }
        public string KustoUri { get; set; }
        public string IngestUri { get; set; }
        public string CertificatePath { get; set; }
        public string CertificatePassword { get; set; }
        public string ApplicationId { get; set; }
        public string TenantId { get; set; }
        public List<Dictionary<string, string>> Data { get; set; }
        public bool AlterTable { get; set; }
        public bool QueryData { get; set; }
        public bool IngestData { get; set; }

        // TODO (config in the configuration file - optional): Change the authentication method from "User Prompt" to any of the other options
        //  Some of the auth modes require additional environment variables to be set in order to work (see usage in generate_connection_string function).
        //  Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
        public string AuthenticationMode { get; set; } // Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)
        // TODO (config - optional): Toggle to False to execute this script "unattended"
        public bool WaitForUser { get; set; }
        public int WaitForIngestSeconds { get; set; }
        public string BatchingPolicy { get; set; }
    }

    public static class Util
    {
        private static int _step = 1;

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
                    ErrorHandler($"File '{configFilePath}' is missing required fields: {string.Join(", ", missing.Select(item => item.name))}");

                if (config.Data is null || !config.Data.Any() || config.Data[0].Count == 0)
                    ErrorHandler($"Required field Data in '{configFilePath}' is either missing, empty or misfilled");
                return config;
            }
            catch (Exception ex)
            {
                ErrorHandler($"Couldn't read config file: '{configFilePath}'", ex);
            }

            return null;
        }

        /// <summary>
        /// Generates Kusto Connection String based on given Authentication Mode.
        /// </summary>
        /// <param name="clusterUrl"> Cluster to connect to.</param>
        /// <param name="authenticationMode">User Authentication Mode, Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)</param>
        /// <param name="certificatePath">Given certificate path</param>
        /// <param name="certificatePassword">Given certificate password</param>
        /// <param name="applicationId">Given application id</param>
        /// <param name="tenantId">Given tenant id</param>
        /// <returns>A connection string to be used when creating a Client</returns>
        public static KustoConnectionStringBuilder GenerateConnectionString(string clusterUrl, string authenticationMode, string certificatePath,
            string certificatePassword, string applicationId, string tenantId)
        {
            // Learn More: For additional information on how to authorize users and apps in Kusto see:
            // https://docs.microsoft.com/azure/data-explorer/manage-database-permissions
            switch (authenticationMode)
            {
                case "UserPrompt":
                    // Prompt user for credentials
                    return new KustoConnectionStringBuilder(clusterUrl).WithAadUserPromptAuthentication();

                case "ManagedIdentity":
                    // Authenticate using a System-Assigned managed identity provided to an azure service, or using a User-Assigned managed identity.
                    // For more information, see https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
                    return CreateManagedIdentityConnectionString(clusterUrl);

                case "AppKey":
                    // Learn More: For information about how to procure an AAD Application,
                    // see: https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
                    // TODO (config - optional): App ID & tenant, and App Key to authenticate with
                    return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationKeyAuthentication(
                        Environment.GetEnvironmentVariable("APP_ID"),
                        Environment.GetEnvironmentVariable("APP_KEY"),
                        Environment.GetEnvironmentVariable("APP_TENANT"));

                case "AppCertificate":
                    // Authenticate using a certificate file.
                    return CreateAppCertificateConnectionString(clusterUrl, certificatePath, certificatePassword, applicationId, tenantId);

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
            return clientId is null
                ? new KustoConnectionStringBuilder(clusterUrl).WithAadSystemManagedIdentity()
                : new KustoConnectionStringBuilder(clusterUrl).WithAadUserManagedIdentity(clientId);
        }


        /// <summary>
        /// Generates Kusto Connection String based on 'AppCertificate' Authentication Mode.
        /// </summary>
        /// <param name="clusterUrl">Url of cluster to connect to</param>
        /// <param name="certificatePath">Given certificate path</param>
        /// <param name="certificatePassword">Given certificate password</param>
        /// <param name="applicationId">Given application id</param>
        /// <param name="tenantId">Given tenant id</param>
        /// <returns>AppCertificate Kusto Connection String</returns>
        public static KustoConnectionStringBuilder CreateAppCertificateConnectionString(string clusterUrl, string certificatePath, string certificatePassword,
            string applicationId, string tenantId)
        {
            var missing = new[]
            {
                (name: nameof(certificatePath), value: certificatePath),
                (name: nameof(certificatePassword), value: certificatePassword),
                (name: nameof(applicationId), value: tenantId),
            }.Where(item => string.IsNullOrWhiteSpace(item.value)).ToArray();
            if (missing.Any())
                ErrorHandler($"Missing the following required fields in configuration file in order to authenticate using a certificate: " +
                             $"{string.Join(", ", missing.Select(item => item.name))}");


            var appId = Environment.GetEnvironmentVariable("APP_ID");
            var appTenant = Environment.GetEnvironmentVariable("APP_TENANT");
            var privateKeyPemFilePath = Environment.GetEnvironmentVariable("PRIVATE_KEY_PEM_FILE_PATH");
            var certThumbprint = Environment.GetEnvironmentVariable("CERT_THUMBPRINT");
            var publicCertFilePath = Environment.GetEnvironmentVariable("PUBLIC_CERT_FILE_PATH");
            string publicCertificate;
            string pemCertificate;

            try
            {
                pemCertificate = File.ReadAllText(privateKeyPemFilePath ?? throw new InvalidOperationException());
            }
            catch (InvalidOperationException e)
            {
                ErrorHandler($"Failed to load PEM file from {privateKeyPemFilePath}", e);
            }

            if (publicCertFilePath != null)
            {
                try
                {
                    publicCertificate = File.ReadAllText(publicCertFilePath);
                }
                catch (InvalidOperationException e)
                {
                    ErrorHandler($"Failed to load public certificate file from {publicCertFilePath}", e);
                }
                // return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationCertificateAuthentication();
            }



            X509Certificate2 certificate = null;
            if (certificatePath != null)
                certificate = new X509Certificate2(certificatePath, certificatePassword);
            else
                ErrorHandler("Missing Certificate path!");

            return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationCertificateAuthentication(applicationId, certificate, tenantId,
                sendX5c: true);
        }

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
        public static async Task<bool> ExecuteControlCommand(ICslAdminProvider adminClient, string configDatabaseName, string command)
        {
            try
            {
                var clientRequestProperties = CreateClientRequestProperties("CS_SampleApp_ControlCommand");
                var result = (await adminClient.ExecuteControlCommandAsync(configDatabaseName, command, clientRequestProperties)).ToJObjects().ToArray();

                // Tip: Actual implementations wouldn't generally print the response from a control command.We print here to demonstrate what a sample of the
                // response looks like.
                // Moreover, there are some built-in classes for control commands under the Kusto.Data namespace -for example,
                // Kusto.Data.TablesShowCommandResult maps to the result of the ".show tables" commands
                Console.WriteLine($"Response from executed control command '{command}':\n--------------------");
                var firstRow = result[0];
                foreach (var item in firstRow.Properties())
                    Console.WriteLine(item);

                return true;
            }
            catch (Exception ex)
            {
                var err = ex.GetType().ToString();
                string msg;
                if (err == "KustoClientException")
                    msg = "Client error while trying to execute control command '{0}' on database '{1}'";
                else if (err == "KustoClientException")
                    msg = "Server error while trying to execute control command '{0}' on database '{1}'";
                else
                    msg = "Unknown error while trying to execute control command '{0}' on database '{1}'";

                Console.WriteLine(msg, command, configDatabaseName);
                Console.WriteLine(ex);
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
        public static async Task<bool> ExecuteQuery(ICslQueryProvider queryClient, string configDatabaseName, string query)
        {
            try
            {
                var clientRequestProperties = CreateClientRequestProperties("CS_SampleApp_Query");
                var result = (await queryClient.ExecuteQueryAsync(configDatabaseName, query, clientRequestProperties)).ToJObjects().ToArray();

                // Tip: Actual implementations wouldn't generally print the response from a query. We print here to demonstrate what a sample of the response
                // looks like.
                Console.WriteLine($"Response from executed query '{query}':\n--------------------");
                var firstRow = result[0];
                foreach (var item in firstRow.Properties())
                    Console.WriteLine(item);

                return true;
            }
            catch (Exception ex)
            {
                var err = ex.GetType().ToString();
                string msg;
                if (err == "KustoClientException")
                    msg = "Client error while trying to execute control command '{0}' on database '{1}'";
                else if (err == "KustoClientException")
                    msg = "Server error while trying to execute control command '{0}' on database '{1}'";
                else
                    msg = "Unknown error while trying to execute control command '{0}' on database '{1}'";

                Console.WriteLine(msg, query, configDatabaseName);
                Console.WriteLine(ex);
            }

            return false;
        }

        /// <summary>
        /// Creates a fitting KustoIngestionProperties object, to be used when executing ingestion commands.
        /// </summary>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="mappingName">Desired mapping name</param>
        /// <returns>KustoIngestionProperties object</returns>
        public static KustoIngestionProperties CreateIngestionProperties(string configDatabaseName, string configTableName, DataSourceFormat dataFormat,
            string mappingName)
        {
            var kustoIngestionProperties = new KustoIngestionProperties()
            {
                DatabaseName = configDatabaseName,
                TableName = configTableName,
                IngestionMapping = new IngestionMapping() { IngestionMappingReference = mappingName },
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
        /// <param name="filePath">File path</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="mappingName">Desired mapping name</param>
        public static async Task IngestFromFile(IKustoIngestClient ingestClient, string configDatabaseName, string configTableName, string filePath,
            DataSourceFormat dataFormat, string mappingName)
        {
            var ingestionProperties = CreateIngestionProperties(configDatabaseName, configTableName, dataFormat, mappingName);

            // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor instead of the default below of
            // 0. Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
            // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere.
            // Tip 3: To instruct the client to ingest a file (and not another source type), we can either provide it with a path as the sourceUri,
            // or use the IsLocalFileSystem = true flag.
            var sourceOptions = new StorageSourceOptions
            {
                Size = 0,
                SourceId = Guid.NewGuid(),
                IsLocalFileSystem = true
            };

            await ingestClient.IngestFromStorageAsync(filePath, ingestionProperties, sourceOptions);
        }

        /// <summary>
        /// Ingest Data from a Blob.
        /// </summary>
        /// <param name="ingestClient">Client to ingest data</param>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="blobUri">Blob Uri</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="mappingName">Desired mapping name</param>
        public static async Task IngestFromBlobAsync(IKustoIngestClient ingestClient, string configDatabaseName, string configTableName, string blobUri,
            DataSourceFormat dataFormat, string mappingName)
        {
            var ingestionProperties = CreateIngestionProperties(configDatabaseName, configTableName, dataFormat, mappingName);

            // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor instead of the default below of
            // 0. Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
            // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere
            var sourceOptions = new StorageSourceOptions() { Size = 0, SourceId = Guid.NewGuid() };
            await ingestClient.IngestFromStorageAsync(blobUri, ingestionProperties, sourceOptions);
        }

        /// <summary>
        /// Halts the program for WaitForIngestSeconds, allowing the queued ingestion process to complete.
        /// </summary>
        public static async Task WaitForIngestionToComplete(int WaitForIngestSeconds)
        {
            Console.WriteLine(
                $"Sleeping {WaitForIngestSeconds} seconds for queued ingestion to complete. Note: This may take longer depending on the file size " +
                $"and ingestion batching policy.");
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

        /// <summary>
        /// Error handling function. Will mention the appropriate error message (and the exception itself if exists), and will quit the program. 
        /// </summary>
        /// <param name="error">Appropriate error message received from calling function</param>
        /// <param name="e">Thrown exception</param>
        public static void ErrorHandler(string error, Exception e = null)
        {
            Console.WriteLine($"Script failed with error: {error}");
            if (e != null)
                Console.WriteLine($"Exception: {e}");

            Environment.Exit(-1);
        }

    }


}