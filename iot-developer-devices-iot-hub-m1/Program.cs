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
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace iot_developer_devices_iot_hub_m1
{
  class Program
  {
    // TODO: set your IoT Hub info here
    private const string deviceId = "TODO";
    private const string deviceConnectionString = "HostName=TODO.azure-devices.net;DeviceId=TODO;SharedAccessKey=TODO";
    private const string iotHubConnectionString = "HostName=TODO.azure-devices.net;SharedAccessKeyName=TODO;SharedAccessKey=TODO";

    private const int maximumQueryPageCount = 2;

    private static readonly ConsoleColor defaultConsoleForegroundColor = Console.ForegroundColor;
    private static readonly object lockObject = new object();

    static async Task Main(string[] args)
    {
      using var registryManager = RegistryManager.CreateFromConnectionString(iotHubConnectionString);
      using var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString);

      await GetCurrentTwin(deviceClient);
      await StartListeningForDeviceTwinChanges(deviceClient);

      while (true)
      {
        ConsoleWriteLine();
        ConsoleWriteLine("*** Press 'T' to send a twin update ***");
        ConsoleWriteLine("*** Press 'Q' followed by a query to find devices ***");
        ConsoleWriteLine("*** Press ENTER to quit ***");
        ConsoleWriteLine();
        var consoleInput = Console.ReadLine();

        if (string.IsNullOrEmpty(consoleInput))
        {
          // quit
          break;
        }
        else if ("T".Equals(consoleInput, StringComparison.InvariantCultureIgnoreCase))
        {
          await UpdateTwin(registryManager, deviceId);

          // a little delay, so the twin change handler can output to the console first
          await Task.Delay(2000);
        }
        else if (consoleInput.StartsWith("Q ", StringComparison.InvariantCultureIgnoreCase))
        {
          var sqlQueryString = consoleInput.Substring(2).Trim();

          await QueryDevices(registryManager, sqlQueryString);
        }
        else
        {
          ConsoleWriteLine("*** Sorry, I didn't understand that input ***", ConsoleColor.Red);
        }
      }
    }

    #region DeviceClient

    private static async Task GetCurrentTwin(DeviceClient deviceClient)
    {
      try
      {
        ConsoleWriteLine();
        ConsoleWriteLine("Will get current twin...", ConsoleColor.Green);

        var twin = await deviceClient.GetTwinAsync();

        ConsoleWriteLine("Successfully got current twin:", ConsoleColor.Green);
        ConsoleWriteLine(twin.ToJson(Formatting.Indented), ConsoleColor.Green);
        ConsoleWriteLine();
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
    }

    private static async Task StartListeningForDeviceTwinChanges(DeviceClient deviceClient)
    {
      try
      {
        ConsoleWriteLine();
        ConsoleWriteLine($"Will setup listener for desired property updates...", ConsoleColor.Green);

        await deviceClient.SetDesiredPropertyUpdateCallbackAsync(
          callback: DesiredPropertyUpdateCallback,
          userContext: deviceClient);

        ConsoleWriteLine($"Now listening for desired property updates", ConsoleColor.Green);
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
    }

    private static async Task DesiredPropertyUpdateCallback(TwinCollection desiredProperties, object userContext)
    {
      try
      {
        var deviceClient = (DeviceClient)userContext;

        ConsoleWriteLine();
        ConsoleWriteLine($"Received twin update, including {desiredProperties.Count} desired properties:", ConsoleColor.Green);
        ConsoleWriteLine(desiredProperties.ToJson(Formatting.Indented), ConsoleColor.Green);
        
        var reportedProperties = new TwinCollection();
        var propertyNames = new List<string>();
        
        // enumerate through the desired properties
        var enumerator = desiredProperties.GetEnumerator();
        while (enumerator.MoveNext())
        {
          var pair = (KeyValuePair<string, object>)enumerator.Current;
          var propertyName = pair.Key;

          // copy desired property to reported properties
          reportedProperties[propertyName] = pair.Value;

          propertyNames.Add(propertyName);
        }

        reportedProperties["lastUpdateInfo"] = 
          $"Last desired property update @ {DateTime.UtcNow} involved these properties: {string.Join(", ", propertyNames)}";

        ConsoleWriteLine();
        ConsoleWriteLine($"Will change reported properties accordingly...", ConsoleColor.Green);
        await deviceClient.UpdateReportedPropertiesAsync(reportedProperties);
        ConsoleWriteLine($"Successfully changed reported properties", ConsoleColor.Green);

        await GetCurrentTwin(deviceClient);
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
    }

    #endregion

    #region RegistryManager

    private static async Task UpdateTwin(RegistryManager registryManager, string deviceId)
    {
      try
      {
        ConsoleWriteLine();
        ConsoleWriteLine("Will update twin...", ConsoleColor.Yellow);

        var random = new Random();
        var twin = await registryManager.GetTwinAsync(deviceId);
        
        twin.Properties.Desired["propertySetFromCode"] = $"property value {random.Next(1, 1001)}";
        twin.Tags["tagSetFromCode"] = $"tag value {random.Next(1, 1001)}";

        twin = await registryManager.UpdateTwinAsync(
          deviceId,
          twin,
          twin.ETag);

        ConsoleWriteLine("Successfully updated twin:", ConsoleColor.Yellow);
        ConsoleWriteLine(twin.ToJson(Formatting.Indented), ConsoleColor.Yellow);
        ConsoleWriteLine();
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
    }

    private static async Task QueryDevices(RegistryManager registryManager, string sqlQueryString)
    {
      try
      {
        ConsoleWriteLine();
        ConsoleWriteLine($"Will execute query {sqlQueryString}...", ConsoleColor.Yellow);

        var query = registryManager.CreateQuery(
          sqlQueryString: sqlQueryString, 
          pageSize: 3);
        
        var pageIndex = -1;

        while (query.HasMoreResults && ++pageIndex < maximumQueryPageCount)
        {
          var deviceJsons = (await query.GetNextAsJsonAsync()).ToList();

          ConsoleWriteLine();
          ConsoleWriteLine($"Found {deviceJsons.Count} device(s) in page {pageIndex + 1}", ConsoleColor.Yellow);
          
          for (var deviceIndex = 0; deviceIndex < deviceJsons.Count; deviceIndex++)
          {
            var deviceJson = deviceJsons[deviceIndex];

            ConsoleWriteLine();
            ConsoleWriteLine($"Device {deviceIndex + 1}/{deviceJsons.Count} of page {pageIndex + 1}:", ConsoleColor.Yellow);
            ConsoleWriteLine(deviceJson, ConsoleColor.Yellow);
          }
        }
      }
      catch (Exception ex)
      {
        ConsoleWriteLine($"* ERROR * {ex.Message}", ConsoleColor.Red);
      }
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
