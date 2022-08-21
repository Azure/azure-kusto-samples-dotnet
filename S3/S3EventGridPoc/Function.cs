using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;
using Kusto.Data;
using Kusto.Data.Net.Client;
using Kusto.Ingest;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace S3EventGridPoc;

public class Function
{
    private readonly IKustoIngestClient m_client;

    IAmazonS3 S3Client { get; set; }

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance.
    /// </summary>
    public Function()
    {
        S3Client = new AmazonS3Client();
        string appId = Environment.GetEnvironmentVariable("AppId");
        string appKey = Environment.GetEnvironmentVariable("AppKey");
        string authority = Environment.GetEnvironmentVariable("AppTenant");
        string clusterUri = Environment.GetEnvironmentVariable("IngestionUri");
        var kustoConnectionStringBuilderDM =
            new KustoConnectionStringBuilder(clusterUri)
            .WithAadApplicationKeyAuthentication(appId, appKey, authority);

        Console.WriteLine($"Initializing an ingest client for {clusterUri}");
        // Create an ingest client
        // Note, that creating a separate instance per ingestion operation is an anti-pattern.
        // IngestClient classes are thread-safe and intended for reuse
        m_client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);
    }

    /// <summary>
    /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
    /// </summary>
    /// <param name="s3Client"></param>
    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;

        string appId = Environment.GetEnvironmentVariable("AppId");
        string appKey = Environment.GetEnvironmentVariable("AppKey");
        string authority = Environment.GetEnvironmentVariable("AppTenant");
        string clusterUri = Environment.GetEnvironmentVariable("IngestionUri");
        var kustoConnectionStringBuilderDM =
            new KustoConnectionStringBuilder(clusterUri)
            .WithAadApplicationKeyAuthentication(appId, appKey, authority);

        Console.WriteLine($"Initializing an ingest client for {clusterUri}");
        // Create an ingest client
        // Note, that creating a separate instance per ingestion operation is an anti-pattern.
        // IngestClient classes are thread-safe and intended for reuse
        m_client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);

    }

    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
    /// to respond to S3 notifications.
    /// </summary>
    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var awsCredentials = Environment.GetEnvironmentVariable("AwsCredentials");
        string table = Environment.GetEnvironmentVariable("TargetTable");
        string database = Environment.GetEnvironmentVariable("TargetDatabase");

        foreach (var record in evnt.Records)
        {
            var s3 = record.S3;
            Console.WriteLine($"[{record.AwsRegion} - {record.EventTime}] Bucket = {s3.Bucket.Name}, Key = {s3.Object.Key}");

            // Ingest from blobs according to the required properties
            var kustoIngestionProperties = new KustoQueuedIngestionProperties(databaseName: database, tableName: table)
            {
                FlushImmediately = true
            };

            var sourceOptions = new StorageSourceOptions() { DeleteSourceOnSuccess = false, Size = s3.Object.Size };
            var uri = $"https://{s3.Bucket.Name}.s3.{record.AwsRegion}.amazonaws.com/{s3.Object.Key}";
            Console.WriteLine($"start to ingest {uri}");
            await m_client.IngestFromStorageAsync(uri:$"{uri};AwsCredentials={awsCredentials}", ingestionProperties: kustoIngestionProperties, sourceOptions);
            Console.WriteLine($"complete to ingest {uri}");
        }
    }
}