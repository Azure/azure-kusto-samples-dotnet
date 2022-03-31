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
        public string? KustoUri { get; set; } = null;
        public string? IngestUri { get; set; } = null;
        public List<Dictionary<string, string>>? Data = null;
        public bool AlterTable { get; set; }
        public bool QueryData { get; set; }
        public bool IngestData { get; set; }
    }


    /// <summary>
    /// 
    /// </summary>
    public static class KustoSampleApp
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

        private static int _step = 1;


        /// <summary>
        /// Main engine - runs the actual script. 
        /// </summary>
        private static void Start()
        {
            var config = load_configs(ConfigFileName);
        }

        /// <summary>
        /// Loads JSON configuration file, and sets the metadata in place. 
        /// </summary>
        /// <param name="configFilePath"> Configuration file path.</param>
        /// <returns>ConfigJson object, allowing access to the metadata fields.</returns>
        private static ConfigJson? load_configs(string configFilePath)
        {
            try
            {
                using var r = new StreamReader(configFilePath);
                var json = r.ReadToEnd();
                var config = JsonConvert.DeserializeObject<ConfigJson>(json);

                // TODO: can be implemented with Any?
                if (config.DatabaseName is null || config.TableName is null || config.TableSchema is null ||
                    config.KustoUri is null || config.IngestUri is null || config.Data is null)
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
        private static void ErrorHandler(string error, Exception? e = null)
        {
            Console.WriteLine(ErrorMsg + error);
            if (e is not null)
                Console.WriteLine(ExceptionMsg + e);

            Environment.Exit(-1);
        }


        public static void Main()
        {
            Console.WriteLine(StartMsg);
            Start();
            Console.WriteLine(EndMsg);
        }
    }
}