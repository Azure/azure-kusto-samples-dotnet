### Prerequisites

1. Set up C# version 10 or higher and .Net version 6.0 on your machine. For instructions, consult a C# environment setup tutorial, like [this](https://www.geeksforgeeks.org/setting-environment-c-sharp/).
2. Set up [NuGet](https://docs.microsoft.com/en-us/nuget/what-is-nuget) (if not already installed with your environment), which is the most popular C# dependency management tool.

### Instructions

4. Download the app as a ZIP file.
5. Extract the app source code.
   **Note**: The configuration parameters defined in the `kusto_sample_config.json` file are preconfigured with the appropriate values for your cluster. Verify that these are correct.
6. Open a command line window and navigate to the folder where you extracted the app.
7. If not using Visual Studio/Rider or any other IDE, do stages 3-5. Otherwise, do stage 6.
8. Run `dotnet build QuickStart -o OUTPUT_PATH` to build the project and its dependencies into a set of binaries, including an executable that can be used to run the application. For more information, click [here](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-build)
9. Make sure the build succeeded without any errors.
10. Go to your selected output path, and run the exe file from the command line window with `QuickStart.exe` or just double click it from the file explorer.
11. Using your IDE of choice - build and run the QuickStart project.

### Troubleshooting

* If you are having trouble running the script from your IDE, first check if the script runs from command line, then consult the troubleshooting references of your IDE.