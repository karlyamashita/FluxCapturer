using System.Diagnostics;
using System.IO.Pipes;
using System.IO.Ports;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace FluxCapturer;

public sealed class FluxTransportHost
{

    private readonly record struct DebugLogEntry(string Line, bool IsError);

    private static readonly Channel<DebugLogEntry> DebugLogChannel =
        Channel.CreateUnbounded<DebugLogEntry>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    private static readonly Task DebugLogWriterTask =
        FcLogging.Enabled && FcLogging.LogToFile ? Task.Run(ProcessDebugLogQueueAsync) : Task.CompletedTask;

    private static string DebugLogPath =>
        Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            "Downloads",
            "FluxCapturer_Debug.log");

    private static void DebugLog(string message)
    {
        if (!FcLogging.Allows(FcLogLevel.Debug)) return;
        string line =
            $"[FC {DateTime.Now:HH:mm:ss.fff}] {message}";

        if (FcLogging.LogToFile) DebugLogChannel.Writer.TryWrite(new DebugLogEntry(line, false));
        else if (FcLogging.LogToConsole) Console.WriteLine(line);
    }

    private static void DebugLogError(string message)
    {
        if (!FcLogging.Allows(FcLogLevel.Error)) return;
        string line =
            $"[FC {DateTime.Now:HH:mm:ss.fff}] ERROR: {message}";

        if (FcLogging.LogToFile) DebugLogChannel.Writer.TryWrite(new DebugLogEntry(line, true));
        else if (FcLogging.LogToConsole) Console.Error.WriteLine(line);
    }

    private static async Task ProcessDebugLogQueueAsync()
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(DebugLogPath)!);
            await using var stream = new FileStream(
                DebugLogPath,
                FileMode.Append,
                FileAccess.Write,
                FileShare.ReadWrite,
                bufferSize: 16 * 1024,
                useAsync: true);
            await using var writer = new StreamWriter(stream) { AutoFlush = true };

            await foreach (DebugLogEntry entry in DebugLogChannel.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                if (FcLogging.LogToConsole)
                {
                    if (entry.IsError) Console.Error.WriteLine(entry.Line);
                    else Console.WriteLine(entry.Line);
                }

                await writer.WriteLineAsync(entry.Line).ConfigureAwait(false);
            }
        }
        catch
        {
            // Diagnostics must never interrupt transport.
        }
    }


    // Device-agnostic serial registry and passive RX subscribers.
    private sealed class ConnectedSerial
    {
        public required SerialPort Serial;
        public readonly object WriteGate = new();
        public readonly StringBuilder ReplyLine = new();
        public PendingReply? Pending;
    }
    private sealed record PendingReply(Func<string, bool> Match, TaskCompletionSource<string> Completion);
    private static readonly object SerialGate = new();
    private static readonly Dictionary<string, ConnectedSerial> SerialPorts = new(StringComparer.OrdinalIgnoreCase);
    internal static event Action<string, ReadOnlyMemory<byte>>? SerialBytesReceived;

    internal static async Task<string> SendSerialCommandAsync(
        string portName, string command, Func<string, bool> replyMatcher,
        TimeSpan timeout, CancellationToken cancellationToken)
    {
        ConnectedSerial port;
        lock (SerialGate)
        {
            if (!SerialPorts.TryGetValue(portName, out port!)) return "ERR DEVICE_NOT_CONNECTED";
        }
        var completion = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (port.WriteGate)
        {
            if (!port.Serial.IsOpen) return "ERR DEVICE_DISCONNECTED";
            if (port.Pending is not null) return "ERR COMMAND_ALREADY_PENDING";
            port.Pending = new PendingReply(replyMatcher, completion);
            try { port.Serial.Write(command.TrimEnd('\r', '\n') + "\r\n"); }
            catch (Exception ex)
            {
                port.Pending = null;
                return "ERR COMMAND_WRITE_FAILED " + ex.Message;
            }
        }
        try { return await completion.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false); }
        catch (TimeoutException) { return "ERR COMMAND_TIMEOUT"; }
        finally
        {
            lock (port.WriteGate)
                if (ReferenceEquals(port.Pending?.Completion, completion)) port.Pending = null;
        }
    }

    private static void NotifySerialRx(string portName, ReadOnlyMemory<byte> bytes)
    {
        ConnectedSerial? port;
        lock (SerialGate) SerialPorts.TryGetValue(portName, out port);
        if (port is not null)
        {
            lock (port.WriteGate)
            {
                foreach (byte b in bytes.Span)
                {
                    if (b == (byte)'\n')
                    {
                        string line = port.ReplyLine.ToString().Trim();
                        port.ReplyLine.Clear();
                        if (port.Pending is { } pending && pending.Match(line))
                        {
                            port.Pending = null;
                            pending.Completion.TrySetResult(line);
                        }
                    }
                    else if (b == (byte)'\r') { }
                    else if (port.ReplyLine.Length < 8192) port.ReplyLine.Append((char)b);
                    else port.ReplyLine.Clear();
                }
            }
        }
        // An adapter failure must never stop raw transport.
        var subscribers = SerialBytesReceived;
        if (subscribers is not null)
            foreach (Action<string, ReadOnlyMemory<byte>> subscriber in subscribers.GetInvocationList())
                try { subscriber(portName, bytes); }
                catch (Exception ex) { DebugLogError("RX adapter: " + ex.Message); }
    }

    public const string PipeName = "KTYLabs_FluxCapturer_Transport_v1";
    private const byte RuntimeTxData = 1;
    private const byte RuntimeRxData = 2;
    private const byte RuntimeTxAck = 3;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var pipe = new NamedPipeServerStream(
                PipeName,
                PipeDirection.InOut,
                NamedPipeServerStream.MaxAllowedServerInstances,
                PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous);

            try
            {
                DebugLog(
                    $"Waiting for transport client...");
                await pipe.WaitForConnectionAsync(cancellationToken).ConfigureAwait(false);
                DebugLog(
                    $"Transport client connected.");
                _ = Task.Run(() => HandleClientAsync(pipe, cancellationToken), CancellationToken.None);
            }
            catch
            {
                pipe.Dispose();
                if (cancellationToken.IsCancellationRequested)
                    break;
                throw;
            }
        }
    }

    private static async Task HandleClientAsync(NamedPipeServerStream pipe, CancellationToken hostToken)
    {
        using (pipe)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(hostToken);
            using var pipeWriteGate = new SemaphoreSlim(1, 1);
            try
            {
            byte[] requestBytes = await ReadFrameAsync(pipe, linked.Token).ConfigureAwait(false);
            FluxOpenRequest request = JsonSerializer.Deserialize<FluxOpenRequest>(requestBytes)
                ?? throw new InvalidOperationException("Invalid Flux Capturer open request.");

            DebugLog(
                $"OPEN request: kind={request.Kind}, " +
                $"port={request.PortName}, baud={request.BaudRate}, dataBits={request.DataBits}, " +
                $"parity={request.Parity}, stopBits={request.StopBits}, handshake={request.Handshake}, " +
                $"DTR={request.DtrEnable}, RTS={request.RtsEnable}");

            if (string.Equals(request.Kind, "serial", StringComparison.OrdinalIgnoreCase))
                await OpenSerial(pipe, pipeWriteGate, request, linked.Token).ConfigureAwait(false);
            else if (string.Equals(request.Kind, "tcp", StringComparison.OrdinalIgnoreCase))
                await OpenTcp(pipe, pipeWriteGate, request, linked.Token).ConfigureAwait(false);
            else
                throw new InvalidOperationException($"Unsupported Flux Capturer transport '{request.Kind}'.");
        }
        catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                DebugLogError(
                    $"Client transport error: {ex}");

                try
                {
                    await WriteResponseAsync(pipe, false, ex.Message, CancellationToken.None).ConfigureAwait(false);
                }
                catch { }
            }
        }
    }

    private static async Task OpenSerial(
        NamedPipeServerStream pipe,
        SemaphoreSlim pipeWriteGate,
        FluxOpenRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.PortName))
            throw new InvalidOperationException("A COM port is required.");

        using var serial = new SerialPort(
            request.PortName,
            request.BaudRate,
            (Parity)request.Parity,
            request.DataBits,
            (StopBits)request.StopBits)
        {
            Handshake = (Handshake)request.Handshake,
            DtrEnable = request.DtrEnable,
            RtsEnable = request.RtsEnable,
            ReadTimeout = 250,
            WriteTimeout = 1000,
            ReadBufferSize = 1 << 20,
            WriteBufferSize = 1 << 16
        };

        DebugLog(
            $"Opening {request.PortName} @ {request.BaudRate:N0}...");

        serial.Open();
        var connectedPort = new ConnectedSerial { Serial = serial };
        lock (SerialGate) SerialPorts[serial.PortName] = connectedPort;

        DebugLog(
            $"OPEN OK: {serial.PortName}, IsOpen={serial.IsOpen}");

        await WriteResponseAsync(pipe, true, null, cancellationToken).ConfigureAwait(false);

        DebugLog(
            $"Starting serial RX/TX pumps for {serial.PortName}.");

        Task rx = PumpSerialRxAsync(serial, pipe, pipeWriteGate, cancellationToken);
        Task tx = PumpSerialTxAsync(pipe, pipeWriteGate, serial, request.Rs485Rts, cancellationToken);

        Task completed;
        try { completed = await Task.WhenAny(rx, tx).ConfigureAwait(false); }
        finally
        {
            lock (SerialGate)
                if (SerialPorts.TryGetValue(serial.PortName, out var registered) &&
                    ReferenceEquals(registered, connectedPort)) SerialPorts.Remove(serial.PortName);
        }

        DebugLog($"Serial pump ended: {(ReferenceEquals(completed, rx) ? "RX" : "TX")} status={completed.Status}");
        // A failed/closed pump terminates the whole transport. Cancel its partner
        // before releasing SerialPort so a subsequent Open can reclaim the COM port.
        // HandleClientAsync owns the linked token and the pipe lifetime.
        if (completed.IsFaulted)
        {
            DebugLogError($"Serial pump fault on {serial.PortName}: {completed.Exception}");
            await completed.ConfigureAwait(false);
        }
        if (completed.IsCanceled && !cancellationToken.IsCancellationRequested)
            throw new IOException($"Serial pump unexpectedly canceled on {serial.PortName}.");
        if (!cancellationToken.IsCancellationRequested)
            throw new IOException($"Serial {(ReferenceEquals(completed, rx) ? "RX" : "TX")} pump ended unexpectedly on {serial.PortName}.");
    }

    private static async Task OpenTcp(
        NamedPipeServerStream pipe,
        SemaphoreSlim pipeWriteGate,
        FluxOpenRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Host) || request.TcpPort <= 0)
            throw new InvalidOperationException("A TCP host and port are required.");

        using var tcp = new TcpClient { NoDelay = true };
        await tcp.ConnectAsync(request.Host, request.TcpPort, cancellationToken).ConfigureAwait(false);
        await WriteResponseAsync(pipe, true, null, cancellationToken).ConfigureAwait(false);

        NetworkStream stream = tcp.GetStream();
        Task rx = PumpStreamRxAsync(stream, pipe, pipeWriteGate, cancellationToken);
        Task tx = PumpStreamTxAsync(pipe, pipeWriteGate, stream, cancellationToken);
        await Task.WhenAny(rx, tx).ConfigureAwait(false);
    }

    private static async Task PumpSerialRxAsync(
        SerialPort serial,
        Stream pipe,
        SemaphoreSlim pipeWriteGate,
        CancellationToken cancellationToken)
    {
        DebugLog(
            $"RX pump START: {serial.PortName}");

        byte[] buffer = new byte[16 * 1024];
        ulong rxSequence = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            int read = await serial.BaseStream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (read <= 0) continue;

            DebugLog(
                $"RX {serial.PortName}: {read} byte(s): " +
                $"{Convert.ToHexString(buffer.AsSpan(0, read))}");

            ulong sequence = ++rxSequence;
            try
            {
                await FluxBoundaryTrace.LogSerialRxAsync(
                    serial.PortName, sequence, buffer.AsMemory(0, read), cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception ex) { DebugLogError($"RX trace error: {ex}"); }

            NotifySerialRx(serial.PortName, buffer.AsMemory(0, read));

            await WriteRuntimeFrameAsync(pipe, pipeWriteGate, RuntimeRxData, sequence,
                buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task PumpSerialTxAsync(
        Stream pipe,
        SemaphoreSlim pipeWriteGate,
        SerialPort serial,
        bool rs485Rts,
        CancellationToken cancellationToken)
    {
        DebugLog(
            $"TX pump START: {serial.PortName}");

        while (!cancellationToken.IsCancellationRequested)
        {
            DebugLog(
                $"TX waiting for CC frame...");

            RuntimeFrame frame = ParseRuntimeFrame(
                await ReadFrameAsync(pipe, cancellationToken).ConfigureAwait(false));

            DebugLog(
                $"TX frame from CC: type={frame.Type}, " +
                $"seq={frame.Sequence}, bytes={frame.Payload.Length}: " +
                $"{Convert.ToHexString(frame.Payload)}");
            if (frame.Type != RuntimeTxData)
                throw new InvalidDataException($"Unexpected Flux runtime frame type {frame.Type}.");

            if (rs485Rts) serial.RtsEnable = true;
            try
            {
                DebugLog(
                    $"TX writing {frame.Payload.Length} byte(s) to {serial.PortName}...");

                // Share the write gate with generic device commands.
                lock (SerialGate)
                {
                    if (SerialPorts.TryGetValue(serial.PortName, out var active))
                    {
                        lock (active.WriteGate) serial.Write(frame.Payload, 0, frame.Payload.Length);
                    }
                    else serial.Write(frame.Payload, 0, frame.Payload.Length);
                }

                if (rs485Rts)
                {
                    await serial.BaseStream.FlushAsync(cancellationToken).ConfigureAwait(false);

                    DateTime deadline = DateTime.UtcNow.AddMilliseconds(Math.Max(1000, serial.WriteTimeout));
                    while (serial.BytesToWrite > 0 && DateTime.UtcNow < deadline)
                        await Task.Delay(1, cancellationToken).ConfigureAwait(false);
                    if (serial.BytesToWrite > 0)
                        throw new TimeoutException("Flux Capturer serial transmit queue did not drain.");
                }

                DebugLog(
                    $"TX WRITE OK: {serial.PortName}, " +
                    $"{frame.Payload.Length} byte(s), BytesToWrite={serial.BytesToWrite}");

                try
                {
                    await FluxBoundaryTrace.LogSerialTxAsync(
                        serial.PortName, frame.Sequence, frame.Payload, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception ex) { DebugLogError($"TX trace error: {ex}"); }

                // Normal serial/VCOM traffic is acknowledged as soon as the OS accepts
                // the write. Do not make CC wait for a full UART drain or diagnostic I/O.
                if (!rs485Rts)
                {
                    await WriteRuntimeFrameAsync(
                        pipe, pipeWriteGate, RuntimeTxAck, frame.Sequence,
                        ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                double charMs = 1000.0 * (1 + serial.DataBits + 2) / Math.Max(1, serial.BaudRate);
                await Task.Delay(Math.Max(1, (int)Math.Ceiling(charMs * 2)), cancellationToken)
                    .ConfigureAwait(false);

                await WriteRuntimeFrameAsync(
                    pipe, pipeWriteGate, RuntimeTxAck, frame.Sequence,
                    ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (rs485Rts)
                {
                    try { serial.RtsEnable = false; } catch { }
                }
            }
        }
    }

    private static async Task PumpStreamRxAsync(
        Stream source,
        Stream pipe,
        SemaphoreSlim pipeWriteGate,
        CancellationToken cancellationToken)
    {
        byte[] buffer = new byte[16 * 1024];
        ulong rxSequence = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            int read = await source.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (read == 0) break;

            ulong sequence = ++rxSequence;
            await FluxBoundaryTrace.LogTcpRxAsync(
                sequence, buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);

            await WriteRuntimeFrameAsync(
                pipe, pipeWriteGate, RuntimeRxData, sequence,
                buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task PumpStreamTxAsync(
        Stream pipe,
        SemaphoreSlim pipeWriteGate,
        Stream destination,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            RuntimeFrame frame = ParseRuntimeFrame(
                await ReadFrameAsync(pipe, cancellationToken).ConfigureAwait(false));
            if (frame.Type != RuntimeTxData)
                throw new InvalidDataException($"Unexpected Flux runtime frame type {frame.Type}.");

            await destination.WriteAsync(frame.Payload, cancellationToken).ConfigureAwait(false);
            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);

            await FluxBoundaryTrace.LogTcpTxAsync(
                frame.Sequence, frame.Payload, cancellationToken).ConfigureAwait(false);

            await WriteRuntimeFrameAsync(
                pipe, pipeWriteGate, RuntimeTxAck, frame.Sequence,
                ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        }
    }

    private static Task WriteResponseAsync(Stream pipe, bool success, string? error, CancellationToken cancellationToken) =>
        WriteFrameAsync(
            pipe,
            JsonSerializer.SerializeToUtf8Bytes(new FluxOpenResponse { Success = success, Error = error }),
            cancellationToken);

    private static async Task WriteRuntimeFrameAsync(
        Stream pipe,
        SemaphoreSlim pipeWriteGate,
        byte type,
        ulong sequence,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        byte[] frame = new byte[9 + payload.Length];
        frame[0] = type;
        BitConverter.TryWriteBytes(frame.AsSpan(1, 8), sequence);
        payload.Span.CopyTo(frame.AsSpan(9));

        await pipeWriteGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await WriteFrameAsync(pipe, frame, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            pipeWriteGate.Release();
        }
    }

    private static RuntimeFrame ParseRuntimeFrame(byte[] frame)
    {
        if (frame.Length < 9)
            throw new InvalidDataException("Invalid Flux Capturer runtime frame.");
        return new RuntimeFrame(frame[0], BitConverter.ToUInt64(frame, 1), frame[9..]);
    }

    private static async Task WriteFrameAsync(Stream stream, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        byte[] length = BitConverter.GetBytes(payload.Length);
        await stream.WriteAsync(length, cancellationToken).ConfigureAwait(false);
        if (payload.Length > 0)
            await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<byte[]> ReadFrameAsync(Stream stream, CancellationToken cancellationToken)
    {
        byte[] lengthBytes = new byte[4];
        await ReadExactlyAsync(stream, lengthBytes, cancellationToken).ConfigureAwait(false);
        int length = BitConverter.ToInt32(lengthBytes, 0);
        if (length < 0 || length > 16 * 1024 * 1024)
            throw new InvalidDataException("Invalid Flux Capturer frame length.");
        byte[] payload = new byte[length];
        if (length > 0)
            await ReadExactlyAsync(stream, payload, cancellationToken).ConfigureAwait(false);
        return payload;
    }

    private static async Task ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset), cancellationToken).ConfigureAwait(false);
            if (read == 0) throw new EndOfStreamException();
            offset += read;
        }
    }

    private readonly record struct RuntimeFrame(byte Type, ulong Sequence, byte[] Payload);

    private sealed class FluxOpenRequest
    {
        public string Kind { get; set; } = string.Empty;
        public string? PortName { get; set; }
        public int BaudRate { get; set; }
        public int Parity { get; set; }
        public int DataBits { get; set; }
        public int StopBits { get; set; }
        public int Handshake { get; set; }
        public bool DtrEnable { get; set; }
        public bool RtsEnable { get; set; }
        public bool Rs485Rts { get; set; }
        public string? Host { get; set; }
        public int TcpPort { get; set; }
    }

    private sealed class FluxOpenResponse
    {
        public bool Success { get; set; }
        public string? Error { get; set; }
    }
}

internal static class FluxBoundaryTrace
{
    private static readonly Channel<string> TraceChannel =
        Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    public static string LogPath { get; } = BuildLogPath();

    private static readonly Task TraceWriterTask =
        FcLogging.Allows(FcLogLevel.Debug) && FcLogging.LogToFile ? Task.Run(ProcessTraceQueueAsync) : Task.CompletedTask;

    public static Task LogSerialTxAsync(
        string portName, ulong sequence, ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken) =>
        LogAsync("SERIAL", "TX", portName, sequence, payload, cancellationToken);

    public static Task LogSerialRxAsync(
        string portName, ulong sequence, ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken) =>
        LogAsync("SERIAL", "RX", portName, sequence, payload, cancellationToken);

    public static Task LogTcpTxAsync(
        ulong sequence, ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken) =>
        LogAsync("TCP", "TX", "-", sequence, payload, cancellationToken);

    public static Task LogTcpRxAsync(
        ulong sequence, ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken) =>
        LogAsync("TCP", "RX", "-", sequence, payload, cancellationToken);

    private static Task LogAsync(
        string transport, string direction, string endpoint, ulong sequence,
        ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (!FcLogging.Allows(FcLogLevel.Debug) || !FcLogging.LogToFile) return Task.CompletedTask;
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled(cancellationToken);

        long qpc = Stopwatch.GetTimestamp();
        string utc = DateTime.UtcNow.ToString(
            "yyyy-MM-ddTHH:mm:ss.fffffffZ",
            System.Globalization.CultureInfo.InvariantCulture);
        string hex = Convert.ToHexString(payload.Span);
        string ascii = ToPrintableAscii(payload.Span);

        string line =
            $"{utc} QPC={qpc} {transport} {direction} Endpoint={endpoint} " +
            $"Seq={sequence} Len={payload.Length} HEX={hex} ASCII={ascii}";

        TraceChannel.Writer.TryWrite(line);
        return Task.CompletedTask;
    }

    private static async Task ProcessTraceQueueAsync()
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(LogPath)!);
            await using var stream = new FileStream(
                LogPath,
                FileMode.Append,
                FileAccess.Write,
                FileShare.ReadWrite,
                bufferSize: 32 * 1024,
                useAsync: true);
            await using var writer = new StreamWriter(stream) { AutoFlush = true };

            await foreach (string line in TraceChannel.Reader.ReadAllAsync().ConfigureAwait(false))
                await writer.WriteLineAsync(line).ConfigureAwait(false);
        }
        catch
        {
            // Diagnostics must never interrupt transport.
        }
    }

    private static string BuildLogPath()
    {
        string downloads = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            "Downloads");

        return Path.Combine(downloads, "FluxCapturer_Transport.log");
    }

    private static string ToPrintableAscii(ReadOnlySpan<byte> payload)
    {
        var builder = new System.Text.StringBuilder(payload.Length);
        foreach (byte value in payload)
        {
            builder.Append(value switch
            {
                0x0D => "<CR>",
                0x0A => "<LF>",
                >= 0x20 and <= 0x7E => (char)value,
                _ => '.'
            });
        }

        return builder.ToString();
    }
}
