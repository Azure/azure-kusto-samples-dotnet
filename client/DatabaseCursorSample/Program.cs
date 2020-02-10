using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Newtonsoft.Json;

namespace HelloKusto
{
    // This sample illustrates how to fetch the cursor value from the @ExtendedProperties table. 
    // The cursor value is returned whenever a query uses one of the cursor functions 
    // (e.g. cursor_after(), cursor_before_or_at())
    //
    // The program should execute in an interactive context (so that on first run the user
    // will get asked to sign in to Azure AD to access the Kusto service).
    class Program
    {
        const string Cluster = "https://help.kusto.windows.net";
        const string Database = "Samples";

        static void Main()
        {
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider($"{Cluster}/{Database};Fed=true"))
            {
                // Query for the current cursor
                var query = "print cursor_current()";
                var currentCursorValue = queryProvider.ExecuteQuery<string>(query).Single();

                Console.WriteLine("Current cursor value is: " + currentCursorValue);

                // Now query for all records after current cursor, and fetch the new cursor from the @ExtendedProperties table
                query = $"StormEvents | where cursor_after('{currentCursorValue}') | count";

                var dataSet = queryProvider.ExecuteQuery(query).ToDataSet();
                var cursor = ExtractCursorValue(dataSet);

                Console.WriteLine("Cursor value in @ExtendedProperties table is: " + cursor);
            }
        }

        public static string ExtractCursorValue(DataSet dataset)
        {
            var tableName = "@ExtendedProperties";
            var tableCount = dataset.Tables.Count;
            var toc = dataset.Tables[tableCount - 1];
            int i = 0;
            foreach (DataRow row in toc.Rows)
            {
                // the 3rd (index=2) column is the table name, we search for the @ExtendedProperties table
                var items = row.ItemArray;
                if (items[2].ToString().Equals(tableName, StringComparison.Ordinal))
                {
                    foreach (DataRow extendedPropertiesRow in dataset.Tables[i].Rows)
                    {
                        if (extendedPropertiesRow[0].ToString().Contains("Cursor"))
                        {
                            Dictionary<string, string> kv = JsonConvert.DeserializeObject<Dictionary<string, string>>(extendedPropertiesRow[0].ToString());
                            return kv["Cursor"];
                        }
                    }
                }
                ++i;
            }
            return null;
        }
    }
}
