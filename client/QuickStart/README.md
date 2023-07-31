# Quickstart App

The quickstart application is a **self-contained and runnable** example app that demonstrates authenticating, connecting to, administering, ingesting data into and querying Azure Data Explorer using the azure-kusto-C# SDK.
You can use it as a baseline to write your own first kusto client application, altering the code as you go, or copy code sections out of it into your app.

**Tip:** The app includes comments with tips on recommendations, coding best practices, links to reference materials and recommended TODO changes when adapting the code to your needs.


## Using the App for the first time

### Prerequisites

1. Set up C# version 7.3 and .Net version 4.72 on your machine. For instructions, consult a .NET Framework setup tutorial, like [this](https://docs.microsoft.com/en-us/dotnet/framework/install/guide-for-developers) or for .Net Core, [this](https://docs.microsoft.com/en-us/dotnet/core/install/windows?tabs=net60).
2. Set up [NuGet](https://docs.microsoft.com/en-us/nuget/what-is-nuget) (if not already installed with your environment), which is the most popular C# dependency management tool.

### Retrieving the app from GitHub

1. Download the app files from this GitHub repo. 
2. Modify the `kusto_sample_config.json` file, changing `KustoUri`, `IngestUri` and `DatabaseName` appropriately for your cluster.

### Retrieving the app from OneClick

1. Open a browser and type your cluster's URL (e.g. https://mycluster.westeurope.kusto.windows.net/). You will be redirected to the _Azure Data Explorer_ Web UI. 
2. On the left menu, select **Data**.
3. Click **Generate Sample App Code** and then follow the instructions in the wizard.
4. Download the app as a ZIP file.
5. Extract the app source code.
   **Note**: The configuration parameters defined in the `kusto_sample_config.json` file are preconfigured with the appropriate values for your cluster. Verify that these are correct.

### Run the app

1. Open a command line window and navigate to the folder where you extracted the app. 
2. Either use an IDE of choice to build and run the project, or do the following using the command line window:
   1. For .net Core:
      1. Run `dotnet build QuickStart -o OUTPUT_PATH` to build the project and its dependencies into a set of binaries, including an executable that can be used to run the application. For more information, click [here](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-build)
   2. For .net Framework:
      1. Locate your MSBuild app and Run `MSBuild build QuickStart.csproj /p:OutputPath=OUTPUT_PATH` to build the project and its dependencies into a set of binaries, including an executable that can be used to run the application. For more information, click [here](https://docs.microsoft.com/en-us/visualstudio/msbuild/msbuild-command-line-reference?view=vs-2022)
   3. Make sure the build succeeded without any errors.
   4. Go to your selected output path, and run the exe file from the command line window with `QuickStart.exe` or just double click it from the file explorer.


#### Troubleshooting

* If you are having trouble running the app from your IDE, first check if the app runs from the command line, then consult the troubleshooting references of your IDE.

### Optional changes

You can make the following changes from within the app itself:

- Change the default User-Prompt authentication method by editing `AUTHENTICATION_MODE`.
- Change the app to run without stopping between steps by setting `WAIT_FOR_USER = false`
