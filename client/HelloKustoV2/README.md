# HelloKustoV2

This repo provides a small C# program that demonstrates the following:

1. Connecting to an Azure Data Explorer (Kusto) cluster
   from C# by using the [Microsoft.Azure.Kusto.Data](https://www.nuget.org/packages/Microsoft.Azure.Kusto.Data)
   NuGet package.

2. Sending a query using the Query V2 API method.

3. Processing the results of the query.

Unlike V1, the Query V2 API (`ICslQueryProvider.ExecuteQueryV2Async`):

1. Is streaming-only (the client doesn't pull all the data into a single
   `System.Data.DataSet` object before delivering it back to the reader).

1. Is async-only.

1. Organizes the different "data tables" in the result in a way that makes
   it easier for the reader to handle. (For example, the query's extended
   properties are delivered first, there's no need to search the data tables
   for the TOC, etc.)

1. Is capable of delivering partial results (called "progressive query" mode).
   (This capability is not shown in this sample, however.)

1. Makes it easier on the reader to determine if the query completed successfully
   or not by providing a summary data frame with all relevant information.
