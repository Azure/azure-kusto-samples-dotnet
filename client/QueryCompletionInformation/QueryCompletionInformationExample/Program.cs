using Kusto.Data;
using Kusto.Data.Data;
using Kusto.Data.Net.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace QueryCompletionInformationExample
{
    /// <summary>
    /// This example demonstrates how to retrieve the QueryCompletionInformation from a Kusto query response stream.
    /// The QueryCompletionInformation is returned as part as any Kusto response and provides information about the query run itself
    /// https://learn.microsoft.com/kusto/api/rest/response-v2?view=microsoft-fabric#the-meaning-of-tables-in-the-response
    /// Here we use the ExecuteQueryV2Async which is the new API with a progressive dataset and KustoDataReaderParser.ParseV2 parses it.
    /// Alternatively one may use ExecuteQueryAsync and ParseV1 and the QueryStatus table.
    /// </summary>
    internal class Program
    {
        static async Task Main(string[] args)
        {
            // Initialize a connection string to a Kusto cluster with AAD User Prompt Authentication.
            // This method prompts the user to sign in to Azure Active Directory (AAD) as part of the authentication process.
            // `clusterUri`: The URI of the Kusto cluster to connect to.

            var clusterUri = "https://help.kusto.windows.net/";
            var kcsb = new KustoConnectionStringBuilder(clusterUri)
                .WithAadUserPromptAuthentication();

            // Create a Kusto client to execute the query.
            using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                // This C# code snippet defines a Kusto query to analyze relationships between employees and their managers.
                // It creates two in-memory tables: `employees` and `reports`.
                // `employees` table contains employee names and ages.
                // `reports` table maps employees to their direct managers.
                // The query then constructs a graph from the `reports` table, linking employees to managers.
                // It uses the `make-graph` operator to create this graph, specifying the relationship direction from employee to manager.
                // Finally, it performs a `graph-match` operation to find all employees under 30 years old who report directly or indirectly to "Alice".
                // The result projects the name and age of these employees, along with their reporting path to Alice.

                var database = "Samples";
                var query = @"let employees = datatable(name:string, age:long) 
[ 
  ""Alice"", 32,  
  ""Bob"", 31,  
  ""Eve"", 27,  
  ""Joe"", 29,  
  ""Chris"", 45, 
  ""Alex"", 35,
  ""Ben"", 23,
  ""Richard"", 39,
]; 
let reports = datatable(employee:string, manager:string) 
[ 
  ""Bob"", ""Alice"",  
  ""Chris"", ""Alice"",  
  ""Eve"", ""Bob"",
  ""Ben"", ""Chris"",
  ""Joe"", ""Alice"", 
  ""Richard"", ""Bob""
]; 
reports 
| make-graph employee --> manager with employees on name 
| graph-match (alice)<-[reports*1..5]-(employee)
  where alice.name == ""Alice"" and employee.age < 30
  project employee = employee.name, age = employee.age, reportingPath = reports.manager";

                List<DataTable> dataTables = new List<DataTable>();

                // Execute the query and retrieve the result.
                using (var response = await kustoClient.ExecuteQueryV2Async(database, query, null))
                {
                    // This C# code snippet is printing only the QueryCompletionInformation table out of the all the tables that are returned in a Kusto response.
                    var QueryCompletionInformationTable = KustoDataReaderParser.ParseV2(response, Guid.NewGuid().ToString(), KustoDataReaderParserTraits.None)[WellKnownDataSet.QueryCompletionInformation].Single().TableData;

                    foreach (DataRow row in QueryCompletionInformationTable.Rows)
                    {
                        foreach (DataColumn column in QueryCompletionInformationTable.Columns)
                        {
                            string columnName = column.ColumnName;
                            object columnValue = row[columnName];
                            Console.WriteLine($"{columnName}: {columnValue}");
                        }
                    }
                }
            }
        }
    }
}