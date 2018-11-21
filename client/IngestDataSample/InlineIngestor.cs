using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace IngestDataSample
{
    /// <summary>
    /// Ingests data into a Kusto table by providing the data inlined
    /// as part of the command itself.
    /// </summary>
    static class InlineIngestor
    {
        public static void IngestCsvFile(string connectionString, string tableName, string path, IEnumerable<string> tags)
        {
            using (var adminProvider = KustoClientFactory.CreateCslAdminProvider(connectionString))
            {
                var csvData = File.ReadAllText(path);
                var command = CslCommandGenerator.GenerateTableIngestPushCommand(
                    tableName, /* compressed: */ true, csvData, tags);
                adminProvider.ExecuteControlCommand(command);
            }
        }
    }
}
