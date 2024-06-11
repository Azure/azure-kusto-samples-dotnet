using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Data;
using Kusto.Data.Net.Client;
using System.Data;
using System.Linq;

namespace QueryCompletionInformationExample
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var clusterUri = "https://help.kusto.windows.net/";
            var kcsb = new KustoConnectionStringBuilder(clusterUri)
                .WithAadUserPromptAuthentication();

            using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
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

                using (var response = kustoClient.ExecuteQuery(database, query, null))
                {
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