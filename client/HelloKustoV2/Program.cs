using System;
using System.Data;
using System.Threading.Tasks;

using Kusto.Cloud.Platform.Data;
using Kusto.Cloud.Platform.Utils;

using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data.Results;

namespace HelloKustoV2
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var arguments = new Arguments();
                CommandLineArgsParser.Parse(args, arguments, autoHelp: true);

                MainImpl(arguments);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception raised: {0}", ex.ToString());
            }
        }

        static void MainImpl(Arguments arguments)
        {
            // 1. Create a connection string to a cluster/database with AAD user authentication
            var cluster = "https://help.kusto.windows.net/";
            var database = "Samples";
            var kcsb = new KustoConnectionStringBuilder(cluster, database)
            {
                FederatedSecurity = true
            };

            // 2. Connect to the Kusto query endpoint and create a query provider
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                // 3. Send a query using the V2 API
                var query = "print Welcome='Hello, World!'; print PI=pi()";
                var properties = new ClientRequestProperties()
                {
                    ClientRequestId = "HelloKustoV2;" + Guid.NewGuid().ToString()
                };

                if (arguments.ProgressiveMode)
                {
                    properties.SetOption(ClientRequestProperties.OptionResultsProgressiveEnabled, true);
                }

                var queryTask = queryProvider.ExecuteQueryV2Async(database, query, properties);

                // 4. Parse and print the results of the query
                WriteResultsToConsole(queryTask);
            }
        }

        static void WriteResultsToConsole(Task<ProgressiveDataSet> queryTask)
        {
            using (var dataSet = queryTask.Result)
            {
                using (var frames = dataSet.GetFrames())
                {
                    var frameNum = 0;
                    while (frames.MoveNext())
                    {
                        var frame = frames.Current;
                        WriteFrameResultsToConsole(frameNum++, frame);
                    }
                }
            }
        }

        static void WriteFrameResultsToConsole(int frameNum, ProgressiveDataSetFrame frame)
        {
            switch (frame.FrameType)
            {
                case FrameType.DataSetHeader:
                    {
                        // This is the first frame we'll get back
                        var frameex = frame as ProgressiveDataSetHeaderFrame;
                        var banner = $"[{frameNum}] DataSet/HeaderFrame: Version={frameex.Version}, IsProgressive={frameex.IsProgressive}";
                        Console.WriteLine(banner);
                        Console.WriteLine();
                    }
                    break;

                case FrameType.TableHeader:
                    // (Progressive mode only)
                    // This frame appears once for each table, before the table's data
                    // is reported.
                    {
                        var frameex = frame as ProgressiveDataSetDataTableSchemaFrame;
                        var banner = $"[{frameNum}] DataTable/SchemaFrame: TableId={frameex.TableId}, TableName={frameex.TableName}, TableKind={frameex.TableKind}";
                        WriteSchema(banner, frameex.TableSchema);
                    }
                    break;

                case FrameType.TableFragment:
                    // (Progressive mode only)
                    // This frame provides one part of the table's data
                    {
                        var frameex = frame as ProgressiveDataSetDataTableFragmentFrame;
                        var banner = $"[{frameNum}] DataTable/FragmentFrame: TableId={frameex.TableId}, FieldCount={frameex.FieldCount}, FrameSubType={frameex.FrameSubType}";
                        WriteProgressiveResults(banner, frameex);
                    }
                    break;

                case FrameType.TableCompletion:
                    // (Progressive mode only)
                    // This frame announces the completion of a table (no more data in it).
                    {
                        var frameex = frame as ProgressiveDataSetTableCompletionFrame;
                        var banner = $"[{frameNum}] DataTable/TableCompletionFrame: TableId={frameex.TableId}, RowCount={frameex.RowCount}";
                        Console.WriteLine(banner);
                        Console.WriteLine();
                    }
                    break;

                case FrameType.TableProgress:
                    // (Progressive mode only)
                    // This frame appears periodically to provide a progress estimateion.
                    {
                        var frameex = frame as ProgressiveDataSetTableProgressFrame;
                        var banner = $"[{frameNum}] DataTable/TableProgressFrame: TableId={frameex.TableId}, TableProgress={frameex.TableProgress}";
                        Console.WriteLine(banner);
                        Console.WriteLine();
                    }
                    break;

                case FrameType.DataTable:
                    {
                        // This frame represents one data table (in all, when progressive results
                        // are not used or there's no need for multiple-frames-per-table).
                        // There are usually multiple such tables in the response, differentiated
                        // by purpose (TableKind).
                        // Note that we can't skip processing the data -- we must consume it.

                        var frameex = frame as ProgressiveDataSetDataTableFrame;
                        var banner = $"[{frameNum}] DataTable/DataTableFrame: TableId={frameex.TableId}, TableName={frameex.TableName}, TableKind={frameex.TableKind}";
                        WriteResults(banner, frameex.TableData);
                    }
                    break;

                case FrameType.DataSetCompletion:
                    {
                        // This is the last frame in the data set.
                        // It provides information on the overall success of the query:
                        // Whether there were any errors, whether it got cancelled mid-stream,
                        // and what exceptions were raised if either is true.
                        var frameex = frame as ProgressiveDataSetCompletionFrame;
                        var banner = $"[{frameNum}] DataSet/CompletionFrame: HasErrors={frameex.HasErrors}, Cancelled={frameex.Cancelled}, Exception={ExtendedString.SafeToString(frameex.Exception)}";
                        Console.WriteLine(banner);
                        Console.WriteLine();
                    }
                    break;

                case FrameType.LastInvalid:
                default:
                    // In general this should not happen
                    break;
            }
        }

        static void WriteResults(string banner, IDataReader reader)
        {
            var writer = new System.IO.StringWriter();
            reader.WriteAsText(banner, true, writer,
                firstOnly: false,
                markdown: false,
                includeWithHeader: "ColumnType",
                includeHeader: true);
            Console.WriteLine(writer.ToString());
        } // WriteResults

        static void WriteProgressiveResults(string banner, ProgressiveDataSetDataTableFragmentFrame frameex)
        {
            Console.WriteLine(banner);
            var record = new object[frameex.FieldCount];
            var first = true;
            while (frameex.GetNextRecord(record))
            {
                foreach (var item in record)
                {
                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        Console.Write(",");
                    }
                    if (item == null)
                    {
                        Console.Write("##null");
                    }
                    else
                    {
                        Console.Write(item.ToString());
                    }
                }
                Console.WriteLine();
            }
            Console.WriteLine();
        }

        static void WriteSchema(string banner, DataTable schema)
        {
            var writer = new System.IO.StringWriter();
            schema.CreateDataReader().WriteAsText(banner, true, writer,
                firstOnly: false,
                markdown: false,
                includeHeader: true
                // TODO: , hideResultSetBanner: true
                );
            Console.WriteLine(writer.ToString());
        } // WriteSchema
    }

    [CommandLineArgs]
    class Arguments
    {
        [CommandLineArg("progressive", "If true, enabled receiving results in progressive mode")]
        public bool ProgressiveMode = false;
    }
}
