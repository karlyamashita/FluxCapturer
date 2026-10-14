using System.Globalization;
using System.IO.Pipes;
using System.Text;

namespace FluxCapturer;

/// <summary>
/// Local control endpoint for ILA. This is a transport bridge, not a Pico capture driver.
/// One UTF-8 line command per connection; CAPTURE carries N CSV sample lines and END.
/// </summary>
public sealed class IlaBridgeHost
{
    public const string PipeName = "KTYLabs_FluxCapturer_ILA_v1";
    private const string IlaPipeName = "KTYLabs_I2CLogicAnalyzer_v1";
    private const int MaxSamples = 100_000;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var server = new NamedPipeServerStream(PipeName, PipeDirection.InOut,
                NamedPipeServerStream.MaxAllowedServerInstances, PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous);
            try
            {
                await server.WaitForConnectionAsync(cancellationToken).ConfigureAwait(false);
                _ = Task.Run(() => HandleAsync(server, cancellationToken), CancellationToken.None);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                server.Dispose();
                break;
            }
            catch
            {
                server.Dispose();
                throw;
            }
        }
    }

    private static async Task HandleAsync(NamedPipeServerStream server, CancellationToken hostToken)
    {
        using (server)
        using (var timeout = CancellationTokenSource.CreateLinkedTokenSource(hostToken))
        {
            timeout.CancelAfter(TimeSpan.FromSeconds(15));
            var ct = timeout.Token;
            try
            {
                using var reader = new StreamReader(server, Encoding.UTF8, false, 4096, leaveOpen: true);
                using var writer = new StreamWriter(server, new UTF8Encoding(false), 4096, leaveOpen: true)
                { AutoFlush = true };
                var command = await reader.ReadLineAsync(ct).ConfigureAwait(false);
                if (command is null) return;

                // Explicit protocol handshake prevents silent use of an outdated FC instance.
                if (command.Equals("HELLO", StringComparison.OrdinalIgnoreCase))
                {
                    await writer.WriteLineAsync("OK FC_ILA_BRIDGE_V2 NEW_CAPTURE").ConfigureAwait(false);
                    return;
                }

                if (command.Equals("STATUS", StringComparison.OrdinalIgnoreCase))
                {
                    var reply = await ForwardAsync("STATUS", null, ct).ConfigureAwait(false);
                    var pico = PicoIlaCapture.Snapshot().Status;
                    await writer.WriteLineAsync(reply is null ? "OK ILA=DISCONNECTED " + pico :
                        $"OK ILA=CONNECTED {pico} {reply}").ConfigureAwait(false);
                    return;
                }

                if (command.Equals("REPLAY", StringComparison.OrdinalIgnoreCase))
                {
                    var capture = PicoIlaCapture.Snapshot().Samples;
                    if (capture is null) { await writer.WriteLineAsync("ERR NO_CAPTURE"); return; }
                    await writer.WriteLineAsync(await SendCaptureAsync(capture, ct) ?? "ERR ILA_NOT_CONNECTED");
                    return;
                }

                if (command.Equals("NEW_CAPTURE", StringComparison.OrdinalIgnoreCase))
                {
                    // The ILA adapter chooses a known device; FC core only sends a generic command.
                    string[] ports = PicoIlaCapture.KnownPorts();
                    string armed = ports.Length switch
                    {
                        0 => "ERR PICO_NOT_CONNECTED_OR_NOT_IDENTIFIED",
                        > 1 => "ERR MULTIPLE_PICO_PORTS",
                        _ => await FluxTransportHost.SendSerialCommandAsync(
                            ports[0], "LOGIC I2C ARM",
                            line => line.StartsWith("OK LOGIC I2C ARMED ", StringComparison.Ordinal) ||
                                    line.StartsWith("ERR LOGIC I2C", StringComparison.Ordinal),
                            TimeSpan.FromSeconds(3), ct).ConfigureAwait(false)
                    };
                    if (!armed.StartsWith("OK LOGIC I2C ARMED ", StringComparison.Ordinal))
                    {
                        await writer.WriteLineAsync(armed).ConfigureAwait(false);
                        return;
                    }
                    var clear = await ForwardAsync("CLEAR", null, ct).ConfigureAwait(false);
                    await writer.WriteLineAsync(clear?.StartsWith("OK", StringComparison.Ordinal) == true
                        ? armed : "ERR ARMED_BUT_ILA_CLEAR_FAILED").ConfigureAwait(false);
                    return;
                }

                if (command.Equals("CLEAR", StringComparison.OrdinalIgnoreCase))
                {
                    var reply = await ForwardAsync("CLEAR", null, ct).ConfigureAwait(false);
                    await writer.WriteLineAsync(reply ?? "ERR ILA_NOT_CONNECTED").ConfigureAwait(false);
                    return;
                }

                if (command.StartsWith("CAPTURE ", StringComparison.OrdinalIgnoreCase) &&
                    int.TryParse(command.AsSpan(8), NumberStyles.None, CultureInfo.InvariantCulture, out int n) &&
                    n is >= 2 and <= MaxSamples)
                {
                    var samples = new List<string>(n);
                    double previousTime = double.NegativeInfinity;
                    for (int i = 0; i < n; i++)
                    {
                        string? line = await reader.ReadLineAsync(ct).ConfigureAwait(false);
                        if (line is null || line.Length > 128 || !ValidateSample(line, ref previousTime))
                        {
                            await writer.WriteLineAsync("ERR INVALID_SAMPLE").ConfigureAwait(false);
                            return;
                        }
                        samples.Add(line);
                    }
                    if (!string.Equals(await reader.ReadLineAsync(ct).ConfigureAwait(false), "END", StringComparison.Ordinal))
                    {
                        await writer.WriteLineAsync("ERR MISSING_END").ConfigureAwait(false);
                        return;
                    }
                    var reply = await ForwardAsync($"CAPTURE {n}", samples, ct).ConfigureAwait(false);
                    await writer.WriteLineAsync(reply ?? "ERR ILA_NOT_CONNECTED").ConfigureAwait(false);
                    return;
                }

                await writer.WriteLineAsync("ERR UNKNOWN_COMMAND").ConfigureAwait(false);
            }
            catch (OperationCanceledException) { /* peer timeout or host shutdown */ }
            catch (IOException) { /* disconnected peer */ }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"ILA bridge error: {ex.Message}");
            }
        }
    }

    internal static Task<string?> SendCaptureAsync(IReadOnlyList<string> samples, CancellationToken ct) =>
        ForwardAsync($"CAPTURE {samples.Count}", samples, ct);

    internal static Task<string?> SendPacketAsync(int session, int packet, IReadOnlyList<string> samples, CancellationToken ct) =>
        ForwardAsync($"PACKET {session} {packet} {samples.Count}", samples, ct);

    private static bool ValidateSample(string line, ref double previousTime)
    {
        string[] parts = line.Split(',');
        if (parts.Length != 3 ||
            !double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out double time) ||
            !double.IsFinite(time) || time <= previousTime ||
            (parts[1] != "0" && parts[1] != "1") ||
            (parts[2] != "0" && parts[2] != "1")) return false;
        previousTime = time;
        return true;
    }

    private static async Task<string?> ForwardAsync(string command, IReadOnlyList<string>? samples, CancellationToken ct)
    {
        using var pipe = new NamedPipeClientStream(".", IlaPipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
        try { await pipe.ConnectAsync(350, ct).ConfigureAwait(false); }
        catch (TimeoutException) { return null; }
        catch (IOException) { return null; }
        using var reader = new StreamReader(pipe, Encoding.UTF8, false, 4096, leaveOpen: true);
        using var writer = new StreamWriter(pipe, new UTF8Encoding(false), 4096, leaveOpen: true)
        { AutoFlush = true };
        await writer.WriteLineAsync(command).ConfigureAwait(false);
        if (samples is not null)
        {
            foreach (var sample in samples) await writer.WriteLineAsync(sample).ConfigureAwait(false);
            await writer.WriteLineAsync("END").ConfigureAwait(false);
        }
        return await reader.ReadLineAsync(ct).ConfigureAwait(false);
    }
}
