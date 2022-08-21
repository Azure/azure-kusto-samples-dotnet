# Automatic Ingestion from S3 to ADX using lambda
## Deploy the lambda
Find instructions [here]( https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-package.html)
## Define AAD application
* Find instructions [here]( https://docs.microsoft.com/azure/active-directory/develop/howto-create-service-principal-portal)
* Define a key for the application and save it.
* Grant the application permissions on your ADX cluster - at least database Ingestor on the database you want to ingest to.
## Define environment variables
* Under Lambda -> Configuration -> Environment variables define the following
*   AppKey - Your AAD app key
*	AppId - Your AAD app Id
*	AppTenant - Your aad app tenant
*   IngestionUri - URL to ingest to your ADX cluster. (https://ingest-{clusterName}.{region}.kusto.windows.net)
*	TargetDatabase - ADX database to ingest to.
*	TargetTable - Table to ingest to.
*	AwsCredentials - Key and secret of AWS account which has permissions on the bucket, separated by comma.
## Define S3 trigger
*	Go to your S3 bucket -> Properties
*	Scroll down to Event notifications and click on Create event notification
*	Define the events you want to notify (e.g. all creation events)
*	Choose your lambda as the destination
## Done
Now whenever you put a blob into this bucket, it will be automatically ingested into your ADX!
