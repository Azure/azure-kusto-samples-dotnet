using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IngestDataSample
{
    class Program
    {
        static void PrintUsageAndExit()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  IngestDataSample.exe KustoLab [database name]");
            Console.WriteLine("  IngestDataSample.exe Upload <connectionString> <tableName> <pathToCsvFile> [<semicolon-separated-list-of-tags>]");
            Console.Read();
            Environment.Exit(0);
        }

        static void Main(string[] args)
        {
            if (args.Length == 0 || args[0] == null)
            {
                PrintUsageAndExit();
            }

            if (args[0].Equals("KustoLab", StringComparison.OrdinalIgnoreCase))
            {
                if (args.Length==2)
                {
                    E2EIngestSample.Run(args[1]);
                }
                else
                {
                    E2EIngestSample.Run();
                }
            }
            else if (args[0].Equals("Upload", StringComparison.OrdinalIgnoreCase))
            {
                if (args.Length != 4 && args.Length != 5)
                {
                    PrintUsageAndExit();
                }

                var connectionString = args[1];
                var tableName = args[2];
                var pathToCsvFile = args[3];
                var tags = new string[0];
                if (args.Length > 4)
                    tags = args[4].Trim().Trim('"').Split(new char[] {';'}, StringSplitOptions.RemoveEmptyEntries); 
                InlineIngestor.IngestCsvFile(connectionString, tableName, pathToCsvFile, tags);
            }
            else
            {
                PrintUsageAndExit();
            }
        }
    }
}
