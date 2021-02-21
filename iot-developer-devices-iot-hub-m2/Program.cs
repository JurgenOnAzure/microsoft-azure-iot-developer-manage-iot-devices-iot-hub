/*
  This demo application accompanies Pluralsight course 'Microsoft Azure IoT Developer: Manage IoT Devices with IoT Hub', 
  by Jurgen Kevelaers. See https://pluralsight.pxf.io/iot-devices-iot-hub.

  MIT License

  Copyright (c) 2021 Jurgen Kevelaers

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Shared;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Text;
using System.Threading.Tasks;

namespace iot_developer_devices_iot_hub_m2
{
  class Program
  {
    // TODO: set your IoT Hub and storage info here
    private const string deviceId = "TODO";
    private const string deviceConnectionString = "HostName=TODO.azure-devices.net;DeviceId=TODO;SharedAccessKey=TODO";
    private const string iotHubConnectionString = "HostName=TODO.azure-devices.net;SharedAccessKeyName=TODO;SharedAccessKey=TODO";
    private const string exportStorageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=TODO;AccountKey=TODO;EndpointSuffix=core.windows.net";

    private const string GetDesiredPropertiesMethodName = "GetDesiredProperties";
    private const string GetReportedPropertiesMethodName = "GetReportedProperties";

    private static readonly ConsoleColor defaultConsoleForegroundColor = Console.ForegroundColor;
    private static readonly object lockObject = new object();

    static async Task Main(string[] args)
    {
      using var registryManager = RegistryManager.CreateFromConnectionString(iotHubConnectionString);
      using var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, Microsoft.Azure.Devices.Client.TransportType.Mqtt); // need Mqtt !
      using var serviceClient = ServiceClient.CreateFromConnectionString(iotHubConnectionString);
      using var jobClient = JobClient.CreateFromConnectionString(iotHubConnectionString);
      
      await StartListeningForDirectMethods(deviceClient);

      while (true)
      {
        ConsoleWriteLine();
        ConsoleWriteLine($"*** Press 'I' followed by a method name ({GetDesiredPropertiesMethodName} or {GetReportedPropertiesMethodName}) to invoke a direct method ***");
        ConsoleWriteLine("*** Press 'E' to export all devices to blob storage ***");
        ConsoleWriteLine("*** Press 'U' followed by a device query condition, to launch a twin update job ***");
        ConsoleWriteLine("*** Press 'S' followed by a blob container name, to get a blob URI including SAS ***");
        ConsoleWriteLine("*** Press ENTER to quit ***");
        ConsoleWriteLine();
        var consoleInput = Console.ReadLine();


        if (string.IsNullOrEmpty(consoleInput))
        {
          // quit
          break;
        }
        else if ("E".Equals(consoleInput, StringComparison.InvariantCultureIgnoreCase))
        {
          await ExportDevices(registryManager);
        }
        else if (consoleInput.StartsWith("U ", StringComparison.InvariantCultureIgnoreCase))
        {
          var queryCondition = consoleInput.Substring(2).Trim();
          await StartTwinUpdateJob(jobClient, queryCondition);
        }
        else if ("U".Equals(consoleInput, StringComparison.InvariantCultureIgnoreCase))
        {
        }
        else if (consoleInput.StartsWith("I ", StringComparison.InvariantCultureIgnoreCase))
        {
          var methodName = consoleInput.Substring(2).Trim();

          await InvokeDirectMethod(serviceClient, methodName);
        }
        else if (consoleInput.StartsWith("S ", StringComparison.InvariantCultureIgnoreCase))
        {
          var blobContainerName = consoleInput.Substring(2).Trim();

          var blobContainerUri = await GetBlobContainerUriWithSas(blobContainerName);
          ConsoleWriteLine($"Blob URI with SAS: {blobContainerUri}", ConsoleColor.Yellow);
        }
        else
        {
          ConsoleWriteLine("*** Sorry, I didn't understand that input ***", ConsoleColor.Red);
        }
      }
    }

    #region DeviceClient

    private static async Task StartListeningForDirectMethods(DeviceClient deviceClient)
    {
      try
      {
        ConsoleWriteLine();
        ConsoleWriteLine($"Will setup listener for direct methods...", ConsoleColor.Green);

        await deviceClient.SetMethodHandlerAsync(
          methodName: GetDesiredPropertiesMethodName,
          methodHandler: DirectMethodCallback,
          userContext: deviceClient);

        await deviceClient.SetMethodHandlerAsync(
          methodName: GetReportedPropertiesMethodName,
          methodHandler: DirectMethodCallback,
          userContext: deviceClient);

        ConsoleWriteLine($"Now listening for direct methods", ConsoleColor.Green);
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
    }

    private static async Task<MethodResponse> DirectMethodCallback(MethodRequest methodRequest, object userContext)
    {
      try
      {
        var deviceClient = (DeviceClient)userContext;

        ConsoleWriteLine();
        ConsoleWriteLine($"Received direct method for method '{methodRequest.Name}'", ConsoleColor.Green);

        var twin = await deviceClient.GetTwinAsync();

        string resultJson = null;

        switch (methodRequest.Name)
        {
          case GetDesiredPropertiesMethodName:
            resultJson = twin.Properties.Desired.ToJson(Newtonsoft.Json.Formatting.Indented);
            break;

          case GetReportedPropertiesMethodName:
            resultJson = twin.Properties.Reported.ToJson(Newtonsoft.Json.Formatting.Indented);
            break;
        }

        ConsoleWriteLine($"Will return method result:", ConsoleColor.Green);
        ConsoleWriteLine(resultJson, ConsoleColor.Green);

        return new MethodResponse(Encoding.UTF8.GetBytes(resultJson), 200); // OK
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }

      return new MethodResponse(500); // error
    }

    #endregion

    #region ServiceClient

    private static async Task InvokeDirectMethod(ServiceClient serviceClient, string methodName)
    {
      try
      {
        ConsoleWriteLine();
        ConsoleWriteLine($"Will invoke direct method '{methodName}'...", ConsoleColor.Cyan);

        var methodResult = await serviceClient.InvokeDeviceMethodAsync(
          deviceId,
          new CloudToDeviceMethod(methodName));

        ConsoleWriteLine();
        ConsoleWriteLine($"Direct method result status: {methodResult.Status}, payload:", ConsoleColor.Cyan);
        ConsoleWriteLine(methodResult.GetPayloadAsJson(), ConsoleColor.Cyan);
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
    }

    #endregion

    #region RegistryManager

    private static async Task ExportDevices(RegistryManager registryManager)
    {
      try
      {
        ConsoleWriteLine();
        ConsoleWriteLine($"Will export all devices...", ConsoleColor.Yellow);

        var blobContainerName = "jobdatafromcode";
        var blobName = $"device-export-{DateTime.UtcNow:MM-dd-HH-mm-ss}";
        var blobContainerUri = await GetBlobContainerUriWithSas(blobContainerName);
        
        ConsoleWriteLine($"Using blob container URI: {blobContainerUri}", ConsoleColor.Yellow);
        
        var job = await registryManager.ExportDevicesAsync(
          exportBlobContainerUri: blobContainerUri,
          outputBlobName: blobName,
          excludeKeys: false);

        while (true)
        {
          job = await registryManager.GetJobAsync(job.JobId);

          ConsoleWriteLine($"Export job status: {job.Status}, progress: {job.Progress}%", ConsoleColor.Yellow);

          if (job.Status == JobStatus.Completed ||
              job.Status == JobStatus.Failed ||
              job.Status == JobStatus.Cancelled)
          {
            // job has stopped
            break;
          }

          await Task.Delay(1000);
        }

        ConsoleWriteLine($"Job has stopped. Status: {job.Status}", ConsoleColor.Yellow);

        if (job.Status == JobStatus.Completed)
        {
          ConsoleWriteLine($"Devices were exported to blob {blobContainerName}\\{blobName}", ConsoleColor.Yellow);
        }
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
    }

    #endregion

    #region JobClient

    private static async Task StartTwinUpdateJob(JobClient jobClient, string queryCondition)
    {
      try
      {
        ConsoleWriteLine();
        ConsoleWriteLine($"Will start twin update job for query {queryCondition}...", ConsoleColor.Yellow);

        var jobId = $"job-{DateTime.UtcNow.Ticks}";

        var twin = new Twin();
        twin.Tags["updateFromJobUtc"] = DateTime.UtcNow;
        twin.Tags["updateFromJobId"] = jobId;
        twin.Properties.Desired["jobId"] = jobId;

        var job = await jobClient.ScheduleTwinUpdateAsync(
          jobId: jobId,
          queryCondition: queryCondition,
          twin: twin,
          startTimeUtc: DateTime.UtcNow,
          maxExecutionTimeInSeconds: (long)TimeSpan.FromMinutes(10).TotalSeconds);

        while (true)
        {
          job = await jobClient.GetJobAsync(job.JobId);

          ConsoleWriteLine($"Update job status: {job.Status} {job.StatusMessage}", ConsoleColor.Yellow);

          if (job.Status == JobStatus.Completed ||
              job.Status == JobStatus.Failed ||
              job.Status == JobStatus.Cancelled)
          {
            // job has stopped
            break;
          }

          await Task.Delay(1000);
        }

        ConsoleWriteLine($"Job has stopped. Status: {job.Status} {job.FailureReason}", ConsoleColor.Yellow);
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
    }

    #endregion

    #region Storage

    private static async Task<string> GetBlobContainerUriWithSas(string blobContainerName)
    {
      var storageAccount = CloudStorageAccount.Parse(exportStorageAccountConnectionString);
      var blobClient = storageAccount.CreateCloudBlobClient();
      var blobContainer = blobClient.GetContainerReference(blobContainerName);

      await blobContainer.CreateIfNotExistsAsync();

      var sasPolicy = new SharedAccessBlobPolicy
      {
        SharedAccessExpiryTime = DateTime.UtcNow.AddHours(1),
        Permissions =
          SharedAccessBlobPermissions.Write |
          SharedAccessBlobPermissions.Read |
          SharedAccessBlobPermissions.Delete
      };

      var sas = blobContainer.GetSharedAccessSignature(sasPolicy);

      var sasUri = blobContainer.Uri + sas;

      return sasUri;
    }

    #endregion

    #region Utility

    private static void ConsoleWriteLine(string message = null, ConsoleColor? foregroundColor = null)
    {
      lock (lockObject)
      {
        Console.ForegroundColor = foregroundColor ?? defaultConsoleForegroundColor;
        Console.WriteLine(message);
        Console.ForegroundColor = defaultConsoleForegroundColor;
      }
    }

    #endregion
  }
}
