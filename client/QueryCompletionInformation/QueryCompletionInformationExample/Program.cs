using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Data;
using Kusto.Data.Net.Client;
using System.Data;
using System.Linq;

namespace QueryCompletionInformationExample
{
    /// <summary>
    /// This example demonstrates how to retrieve the QueryCompletionInformation from a Kusto query response stream.
    /// The goal is to process a specific table named "Table_2" (QueryCompletionInformation) from the response stream and print its column names and values to the console.
    /// </summary>
    internal class Program
    {
        static void Main(string[] args)
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
                using (var response = kustoClient.ExecuteQuery(database, query, null))
                {
                    // This C# code snippet is designed to process a sequence of data tables loaded from a response stream.
                    // It continuously loads data tables from the response until the response is closed.
                    // For each data table loaded, it checks if the table's name is "Table_2" (intersting table for Query completion information)
                    // If a table named "Table_2" is found, it iterates through each row of the table.
                    // For each row, it iterates through all columns, printing the column name and its corresponding value to the console.
                    // After processing "Table_2", it breaks out of the loop, ignoring any subsequent tables in the response.
                    // This snippet is typically used in scenarios where data is streamed from a database or a similar data source,
                    // and there's a need to process a specific table from the stream.
                    do
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(response);

                        if (dataTable.TableName.Equals("Table_2"))
                        {
                            foreach (DataRow row in dataTable.Rows)
                            {
                                foreach (DataColumn column in dataTable.Columns)
                                {
                                    string columnName = column.ColumnName;
                                    object columnValue = row[columnName];
                                    Console.WriteLine($"{columnName}: {columnValue}");
                                }
                            }

                            break;
                        }
                    } while (!response.IsClosed);
                }
            }
        }
    }
}