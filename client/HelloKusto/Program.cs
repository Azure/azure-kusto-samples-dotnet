using System;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace HelloKusto
{
    // This sample illustrates how to query Kusto using the Kusto.Data .NET library.
    //
    // For the purpose of demonstration, the query being sent retrieves multiple result sets.
    //
    // The program should execute in an interactive context (so that on first run the user
    // will get asked to sign in to Azure AD to access the Kusto service).
    class Program
    {
        const string Cluster = "https://help.kusto.windows.net";
        const string Database = "Samples";

        static void Main()
        {
            // The query provider is the main interface to use when querying Kusto.
            // It is recommended that the provider be created once for a specific target database,
            // and then be reused many times (potentially across threads) until it is disposed-of.
            var kcsb = new KustoConnectionStringBuilder(Cluster, Database)
                .WithAadUserPromptAuthentication();
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                // The query -- Note that for demonstration purposes, we send a query that asks for two different
                // result sets (HowManyRecords and SampleRecords).
                var query = "StormEvents | count | as HowManyRecords; StormEvents | limit 10 | project StartTime, EventType, State | as SampleRecords";

                // It is strongly recommended that each request has its own unique
                // request identifier. This is mandatory for some scenarios (such as cancelling queries)
                // and will make troubleshooting easier in others.
                var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };
                using (var reader = queryProvider.ExecuteQuery(query, clientRequestProperties))
                {
                    // Read HowManyRecords
                    while (reader.Read())
                    {
                        var howManyRecords = reader.GetInt64(0);
                        Console.WriteLine($"There are {howManyRecords} records in the table");
                    }

                    // Move on to the next result set, SampleRecords
                    reader.NextResult();
                    Console.WriteLine();
                    while (reader.Read())
                    {
                        // Important note: For demonstration purposes we show how to read the data
                        // using the "bare bones" IDataReader interface. In a production environment
                        // one would normally use some ORM library to automatically map the data from
                        // IDataReader into a strongly-typed record type (e.g. Dapper.Net, AutoMapper, etc.)
                        DateTime time = reader.GetDateTime(0);
                        string type = reader.GetString(1);
                        string state = reader.GetString(2);
                        Console.WriteLine("{0}\t{1,-20}\t{2}", time, type, state);
                    }
                }
            }
        }
    }
}
