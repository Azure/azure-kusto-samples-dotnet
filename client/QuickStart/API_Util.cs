using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using Newtonsoft.Json;
using Kusto.Cloud.Platform.Data;
using ShellProgressBar;

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
    /// Util static class - Handels the communication with the API, and provides generic and simple "plug-n-play" functions to use in different programs.
    /// </summary>
    public static class Util
    {
        private const string MgmtPrefix = ".";
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
        public static KustoConnectionStringBuilder GenerateConnectionString(string clusterUrl, string authenticationMode, string certificatePath, string certificatePassword, string applicationId, string tenantId)
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
                    return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationKeyAuthentication(Environment.GetEnvironmentVariable("APP_ID"), Environment.GetEnvironmentVariable("APP_KEY"), Environment.GetEnvironmentVariable("APP_TENANT"));

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
            return clientId is null ? new KustoConnectionStringBuilder(clusterUrl).WithAadSystemManagedIdentity() : new KustoConnectionStringBuilder(clusterUrl).WithAadUserManagedIdentity(clientId);
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
        public static KustoConnectionStringBuilder CreateAppCertificateConnectionString(string clusterUrl, string certificatePath, string certificatePassword, string applicationId, string tenantId)
        {
            var missing = new[]
            {
                (name: nameof(certificatePath), value: certificatePath),
                (name: nameof(certificatePassword), value: certificatePassword),
                (name: nameof(applicationId), value: tenantId),
            }.Where(item => string.IsNullOrWhiteSpace(item.value)).ToArray();
            if (missing.Any())
                ErrorHandler($"Missing the following required fields in configuration file in order to authenticate using a certificate: {string.Join(", ", missing.Select(item => item.name))}");

            var appId = Environment.GetEnvironmentVariable("APP_ID");
            var SubjectDistinguishedName = Environment.GetEnvironmentVariable("SUBJECT_DISTINGUISHED_NAME");
            var IssuerDistinguishedName = Environment.GetEnvironmentVariable("ISSUER_DISTINGUISHED_NAME");
            var privateKeyPemFilePath = Environment.GetEnvironmentVariable("PRIVATE_KEY_PEM_FILE_PATH");
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
                return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationSubjectAndIssuerAuthentication(appId, SubjectDistinguishedName, IssuerDistinguishedName, tenantId);
            }


            X509Certificate2 certificate = null;
            if (certificatePath != null)
                certificate = new X509Certificate2(certificatePath, certificatePassword);
            else
                ErrorHandler("Missing Certificate path!");

            return new KustoConnectionStringBuilder(clusterUrl).WithAadApplicationCertificateAuthentication(applicationId, certificate, tenantId, sendX5c: true);
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
                Newtonsoft.Json.Linq.JObject[] result;
                if (command.StartsWith(MgmtPrefix)) // All control commands start with a specific prefix (usually '.') - and require a different client and scope to execute with.
                {
                    var clientRequestProperties = CreateClientRequestProperties("CS_SampleApp_ControlCommand");
                    ICslAdminProvider adminClient = (ICslAdminProvider)client;
                    result = (await adminClient.ExecuteControlCommandAsync(configDatabaseName, command, clientRequestProperties)).ToJObjects().ToArray();
                }
                else
                {
                    var clientRequestProperties = CreateClientRequestProperties("CS_SampleApp_Query");
                    ICslQueryProvider queryClient = (ICslQueryProvider)client;
                    result = (await queryClient.ExecuteQueryAsync(configDatabaseName, command, clientRequestProperties)).ToJObjects().ToArray();
                }

                // Tip: Actual implementations wouldn't generally print the response from a control command or a query .We print here to demonstrate what a sample of the response looks like.
                // Moreover, there are some built-in classes for control commands under the Kusto.Data namespace -for example, Kusto.Data.TablesShowCommandResult maps to the result of the ".show tables" commands
                Console.WriteLine($"Response from executed command '{command}':\n--------------------");
                var firstRow = result[0];
                foreach (var item in firstRow.Properties())
                    Console.WriteLine(item);
            }
            catch (Exception ex)
            {
                var err = ex.GetType().ToString();
                string msg;
                if (err == "KustoClientException")
                    msg = "Client error while trying to execute command '{0}' on database '{1}'";
                else if (err == "KustoClientException")
                    msg = "Server error while trying to execute command '{0}' on database '{1}'";
                else
                    msg = "Unknown error while trying to execute command '{0}' on database '{1}'";

                ErrorHandler(string.Format(msg, command, configDatabaseName), ex);
            }
        }

        /// <summary>
        /// Creates a fitting KustoIngestionProperties object, to be used when executing ingestion commands.
        /// </summary>
        /// <param name="configDatabaseName">DB name</param>
        /// <param name="configTableName">Table name</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="mappingName">Desired mapping name</param>
        /// <returns>KustoIngestionProperties object</returns>
        public static KustoIngestionProperties CreateIngestionProperties(string configDatabaseName, string configTableName, DataSourceFormat dataFormat, string mappingName)
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
        /// <param name="uri">Uri to ingest from</param>
        /// <param name="dataFormat">Given data format</param>
        /// <param name="mappingName">Desired mapping name</param>
        /// <param name="isFile">Flag indicating whether the uri is of a file or not.</param>
        public static async Task IngestAsync(IKustoIngestClient ingestClient, string configDatabaseName, string configTableName, string uri, DataSourceFormat dataFormat, string mappingName, bool isFile = false)
        {
            var ingestionProperties = CreateIngestionProperties(configDatabaseName, configTableName, dataFormat, mappingName);
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
                sourceOptions.IsLocalFileSystem = true;

            await ingestClient.IngestFromStorageAsync(uri, ingestionProperties, sourceOptions);
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