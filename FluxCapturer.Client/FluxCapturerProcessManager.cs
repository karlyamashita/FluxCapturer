using System.Diagnostics;

namespace FluxCapturer.Client;

public static class FluxCapturerProcessManager
{
    public static async Task EnsureHostRunningAsync(
        CancellationToken cancellationToken = default)
    {
        await Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (FluxCapturerClient.IsAvailable())
                return;

            string executable = FindExecutable();
            var startInfo = new ProcessStartInfo
            {
                FileName = executable,
                WorkingDirectory = Path.GetDirectoryName(executable) ?? AppContext.BaseDirectory,
                UseShellExecute = false,
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden
            };

            using var process = Process.Start(startInfo)
                ?? throw new InvalidOperationException("Windows could not start FluxCapturer.exe.");

            DateTime deadline = DateTime.UtcNow.AddSeconds(5);
            while (!FluxCapturerClient.IsAvailable())
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (process.HasExited)
                {
                    // A simultaneous SF/CC startup may lose the single-instance
                    // mutex race. If the winning host is now available, reuse it.
                    if (FluxCapturerClient.IsAvailable())
                        return;

                    throw new InvalidOperationException(
                        $"FluxCapturer.exe exited during startup with code {process.ExitCode}.");
                }

                if (DateTime.UtcNow >= deadline)
                    throw new TimeoutException(
                        "FluxCapturer.exe did not publish its shared-memory host within 5 seconds.");

                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            Gate.Release();
        }
    }

    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static Process? _ownedProcess;
    private static string? _ownedPortName;
    private static int _leaseCount;

    public static async Task<FluxCapturerProcessLease> AcquireAsync(
        string portName,
        int baudRate,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(portName))
            throw new ArgumentException("A COM port is required.", nameof(portName));
        if (baudRate <= 0)
            throw new ArgumentOutOfRangeException(nameof(baudRate));

        await Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            CleanupExitedOwnedProcess();

            if (FluxCapturerClient.IsAvailable())
            {
                if (_ownedProcess is not null)
                {
                    if (!string.Equals(_ownedPortName, portName, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new InvalidOperationException(
                            $"Flux Capturer is already running for {_ownedPortName}; it cannot also own {portName}.");
                    }

                    _leaseCount++;
                    return new FluxCapturerProcessLease(_ownedProcess.Id);
                }

                return new FluxCapturerProcessLease(0);
            }

            string executable = FindExecutable();
            var startInfo = new ProcessStartInfo
            {
                FileName = executable,
                WorkingDirectory = Path.GetDirectoryName(executable) ?? AppContext.BaseDirectory,
                UseShellExecute = false,
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden
            };
            startInfo.ArgumentList.Add(portName);
            startInfo.ArgumentList.Add(baudRate.ToString(System.Globalization.CultureInfo.InvariantCulture));

            var process = new Process
            {
                StartInfo = startInfo,
                EnableRaisingEvents = true
            };

            if (!process.Start())
            {
                process.Dispose();
                throw new InvalidOperationException("Windows could not start FluxCapturer.exe.");
            }

            try
            {
                DateTime deadline = DateTime.UtcNow.AddSeconds(5);
                while (!FluxCapturerClient.IsAvailable())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (process.HasExited)
                    {
                        throw new InvalidOperationException(
                            $"FluxCapturer.exe exited during startup with code {process.ExitCode}.");
                    }

                    if (DateTime.UtcNow >= deadline)
                        throw new TimeoutException("FluxCapturer.exe did not create its shared memory within 5 seconds.");

                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
                TryStop(process);
                process.Dispose();
                throw;
            }

            _ownedProcess = process;
            _ownedPortName = portName;
            _leaseCount = 1;
            return new FluxCapturerProcessLease(process.Id);
        }
        finally
        {
            Gate.Release();
        }
    }

    internal static void Release(int processId)
    {
        Gate.Wait();
        try
        {
            CleanupExitedOwnedProcess();
            if (_ownedProcess is null || _ownedProcess.Id != processId)
                return;

            if (_leaseCount > 0)
                _leaseCount--;

            if (_leaseCount != 0)
                return;

            TryStop(_ownedProcess);
            _ownedProcess.Dispose();
            _ownedProcess = null;
            _ownedPortName = null;
        }
        finally
        {
            Gate.Release();
        }
    }

    private static string FindExecutable()
    {
        string[] candidates =
        [
            Path.Combine(AppContext.BaseDirectory, "FluxCapturer", "FluxCapturer.exe"),
            Path.Combine(AppContext.BaseDirectory, "FluxCapturer.exe")
        ];

        foreach (string candidate in candidates)
        {
            if (File.Exists(candidate))
                return candidate;
        }

        throw new FileNotFoundException(
            "FluxCapturer.exe was not found. Rebuild or reinstall Serial Flux/CAN Coyote with the Flux Capturer component.",
            candidates[0]);
    }

    private static void CleanupExitedOwnedProcess()
    {
        if (_ownedProcess is null || !_ownedProcess.HasExited)
            return;

        _ownedProcess.Dispose();
        _ownedProcess = null;
        _ownedPortName = null;
        _leaseCount = 0;
    }

    private static void TryStop(Process process)
    {
        try
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit(2_000);
            }
        }
        catch
        {
            // Process cleanup is best-effort; never terminate an unrelated process.
        }
    }
}
