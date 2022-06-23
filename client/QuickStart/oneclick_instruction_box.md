### Prerequisites

1. Set up C# version 7.3 and .Net version 4.72 on your machine. For instructions, consult a .NET Framework setup tutorial, like [this](https://docs.microsoft.com/en-us/dotnet/framework/install/guide-for-developers) or for .Net Core, [this](https://docs.microsoft.com/en-us/dotnet/core/install/windows?tabs=net60).
2. Set up [NuGet](https://docs.microsoft.com/en-us/nuget/what-is-nuget) (if not already installed with your environment), which is the most popular C# dependency management tool.

### Instructions

1. Download the app as a ZIP file.
2. Extract the app source code.
   **Note**: The configuration parameters defined in the `kusto_sample_config.json` file are preconfigured with the appropriate values for your cluster. Verify that these are correct.
3. Open a command line window and navigate to the folder where you extracted the app.
4. Either use an IDE of choice to build and run the project, or do the following using the command line window:
   1. For .net Core:
      1. Run `dotnet build QuickStart -o OUTPUT_PATH` to build the project and its dependencies into a set of binaries, including an executable that can be used to run the application. For more information, click [here](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-build)
   2. For .net Framework:
      1. Locate your MSBuild app and Run `MSBuild build QuickStart.csproj /p:OutputPath=OUTPUT_PATH` to build the project and its dependencies into a set of binaries, including an executable that can be used to run the application. For more information, click [here](https://docs.microsoft.com/en-us/visualstudio/msbuild/msbuild-command-line-reference?view=vs-2022)
   3. Make sure the build succeeded without any errors.
   4. Go to your selected output path, and run the exe file from the command line window with `QuickStart.exe` or just double click it from the file explorer.


### Troubleshooting

* If you are having trouble running the script from your IDE, first check if the script runs from command line, then consult the troubleshooting references of your IDE.