using System.Diagnostics;
using FluxCapturer;

FcLogging.Initialize(args);
bool standalone = args.Any(arg => arg.Equals("--standalone", StringComparison.OrdinalIgnoreCase));
string[] applicationArgs = args.Where((arg, index) =>
    !arg.Equals("--standalone", StringComparison.OrdinalIgnoreCase) &&
    !arg.Equals("--log-level", StringComparison.OrdinalIgnoreCase) &&
    (index == 0 || !args[index - 1].Equals("--log-level", StringComparison.OrdinalIgnoreCase))).ToArray();

const string MutexName = @"Local\KTYLabs_FluxCapturer_SingleInstance_v1";
using var singleInstance = new Mutex(initiallyOwned: true, MutexName, out bool createdNew);
if (!createdNew)
    return 0;

using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};

try
{
    if (applicationArgs.Length == 0)
    {
        using var server = new FluxCapturerServer();
        var transportHost = new FluxTransportHost();
        Console.WriteLine("Flux Capturer host ready.");
        Task sharedMemoryTask = server.RunAsync(cancellation.Token);
        Task transportTask = transportHost.RunAsync(cancellation.Token);
        if (standalone)
        {
            Console.WriteLine("Flux Capturer standalone mode. Press Ctrl+C to exit.");
            await Task.WhenAll(sharedMemoryTask, transportTask);
        }
        else
        {
            Task ownerMonitorTask = MonitorOwnerApplicationsAsync(cancellation);
            await Task.WhenAll(sharedMemoryTask, transportTask, ownerMonitorTask);
        }
        return 0;
    }

    if (applicationArgs.Length == 2 &&
        !string.IsNullOrWhiteSpace(applicationArgs[0]) &&
        int.TryParse(applicationArgs[1], out int baudRate) &&
        baudRate > 0)
    {
        using var server = new FluxCapturerServer(applicationArgs[0].Trim(), baudRate);
        Console.WriteLine($"Flux Capturer: {server.PortName} @ {server.BaudRate:N0} baud");
        await server.RunAsync(cancellation.Token);
        return 0;
    }

    Console.Error.WriteLine("Usage: FluxCapturer.exe [--standalone] | [<COM port> <baud rate>]");
    return 2;
}
catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
{
    return 0;
}
catch (Exception exception)
{
    Console.Error.WriteLine($"Flux Capturer error: {exception.Message}");
    return 1;
}

static async Task MonitorOwnerApplicationsAsync(CancellationTokenSource cancellation)
{
    // The host is started by Serial Flux or CAN Coyote X7. Give the launching
    // application a short startup grace period, then keep FC alive only while
    // at least one of those applications is still running.
    await Task.Delay(1_000, cancellation.Token).ConfigureAwait(false);

    while (!cancellation.IsCancellationRequested)
    {
        bool serialFluxRunning = IsProcessRunning("SerialFlux");
        bool canCoyoteRunning = IsProcessRunning("CanCoyoteX7");

        if (!serialFluxRunning && !canCoyoteRunning)
        {
            Console.WriteLine("Flux Capturer: Serial Flux and CAN Coyote X7 are closed; exiting.");
            cancellation.Cancel();
            return;
        }

        await Task.Delay(500, cancellation.Token).ConfigureAwait(false);
    }
}

static bool IsProcessRunning(string processName)
{
    Process[] processes = Process.GetProcessesByName(processName);
    try
    {
        return processes.Length != 0;
    }
    finally
    {
        foreach (Process process in processes)
            process.Dispose();
    }
}
