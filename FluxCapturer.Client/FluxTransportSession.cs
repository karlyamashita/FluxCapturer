using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text;
using System.Text.Json;

namespace FluxCapturer.Client;

public sealed class FluxTransportSession : IAsyncDisposable
{
    public const string PipeName = "KTYLabs_FluxCapturer_Transport_v1";
    private const byte RuntimeTxData = 1;
    private const byte RuntimeRxData = 2;
    private const byte RuntimeTxAck = 3;

    private NamedPipeClientStream? _pipe;
    private CancellationTokenSource? _readCts;
    private Task? _readTask;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> _pendingTxAcks = new();
    private long _nextTxSequence;

    public bool IsConnected => _pipe?.IsConnected == true;
    public event EventHandler<byte[]>? DataReceived;
    public event EventHandler<string>? Faulted;

    public Task OpenSerialAsync(
        string portName,
        int baudRate,
        int parity,
        int dataBits,
        int stopBits,
        int handshake,
        bool dtrEnable,
        bool rtsEnable,
        bool rs485Rts,
        CancellationToken cancellationToken = default) =>
        OpenAsync(new FluxOpenRequest
        {
            Kind = "serial",
            PortName = portName,
            BaudRate = baudRate,
            Parity = parity,
            DataBits = dataBits,
            StopBits = stopBits,
            Handshake = handshake,
            DtrEnable = dtrEnable,
            RtsEnable = rtsEnable,
            Rs485Rts = rs485Rts
        }, cancellationToken);

    public Task OpenTcpAsync(
        string host,
        int port,
        CancellationToken cancellationToken = default) =>
        OpenAsync(new FluxOpenRequest
        {
            Kind = "tcp",
            Host = host,
            TcpPort = port
        }, cancellationToken);

    private async Task OpenAsync(FluxOpenRequest request, CancellationToken cancellationToken)
    {
        await DisposeConnectionAsync().ConfigureAwait(false);

        var pipe = new NamedPipeClientStream(
            ".",
            PipeName,
            PipeDirection.InOut,
            PipeOptions.Asynchronous);

        await pipe.ConnectAsync(5_000, cancellationToken).ConfigureAwait(false);
        byte[] json = JsonSerializer.SerializeToUtf8Bytes(request);
        await WriteFrameAsync(pipe, json, cancellationToken).ConfigureAwait(false);

        byte[] responseBytes = await ReadFrameAsync(pipe, cancellationToken).ConfigureAwait(false);
        var response = JsonSerializer.Deserialize<FluxOpenResponse>(responseBytes)
            ?? throw new InvalidOperationException("Flux Capturer returned an invalid open response.");

        if (!response.Success)
        {
            pipe.Dispose();
            throw new InvalidOperationException(response.Error ?? "Flux Capturer could not open the transport.");
        }

        _pipe = pipe;
        _nextTxSequence = 0;
        _readCts = new CancellationTokenSource();
        _readTask = Task.Run(() => ReadLoopAsync(pipe, _readCts.Token), CancellationToken.None);
    }

    public async Task SendAsync(byte[] data, CancellationToken cancellationToken = default)
    {
        if (_pipe?.IsConnected != true)
            throw new InvalidOperationException("Flux Capturer transport is not connected.");

        ulong sequence = unchecked((ulong)Interlocked.Increment(ref _nextTxSequence));
        var completion = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        if (!_pendingTxAcks.TryAdd(sequence, completion))
            throw new InvalidOperationException("Flux Capturer transmit sequence collision.");

        try
        {
            await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                byte[] frame = BuildRuntimeFrame(RuntimeTxData, sequence, data);
                await WriteFrameAsync(_pipe, frame, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _writeGate.Release();
            }

            await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _pendingTxAcks.TryRemove(sequence, out _);
        }
    }

    private async Task ReadLoopAsync(NamedPipeClientStream pipe, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested && pipe.IsConnected)
            {
                RuntimeFrame frame = ParseRuntimeFrame(
                    await ReadFrameAsync(pipe, cancellationToken).ConfigureAwait(false));

                if (frame.Type == RuntimeRxData)
                {
                    if (frame.Payload.Length > 0)
                        DataReceived?.Invoke(this, frame.Payload);
                    continue;
                }

                if (frame.Type == RuntimeTxAck)
                {
                    if (_pendingTxAcks.TryGetValue(frame.Sequence, out var completion))
                        completion.TrySetResult(true);
                    continue;
                }

                throw new InvalidDataException($"Unexpected Flux runtime frame type {frame.Type}.");
            }
        }
        catch (OperationCanceledException) { }
        catch (EndOfStreamException) { }
        catch (IOException) { }
        catch (Exception ex)
        {
            FailPendingSends(ex);
            Faulted?.Invoke(this, ex.Message);
        }
    }

    private static byte[] BuildRuntimeFrame(byte type, ulong sequence, ReadOnlySpan<byte> payload)
    {
        byte[] frame = new byte[9 + payload.Length];
        frame[0] = type;
        BitConverter.TryWriteBytes(frame.AsSpan(1, 8), sequence);
        payload.CopyTo(frame.AsSpan(9));
        return frame;
    }

    private static RuntimeFrame ParseRuntimeFrame(byte[] frame)
    {
        if (frame.Length < 9)
            throw new InvalidDataException("Invalid Flux Capturer runtime frame.");
        return new RuntimeFrame(frame[0], BitConverter.ToUInt64(frame, 1), frame[9..]);
    }

    private static async Task WriteFrameAsync(Stream stream, byte[] payload, CancellationToken cancellationToken)
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
            if (read == 0)
                throw new EndOfStreamException();
            offset += read;
        }
    }

    private void FailPendingSends(Exception exception)
    {
        foreach (var pair in _pendingTxAcks)
            pair.Value.TrySetException(exception);
    }

    private async Task DisposeConnectionAsync()
    {
        CancellationTokenSource? cts = _readCts;
        Task? readTask = _readTask;
        NamedPipeClientStream? pipe = _pipe;
        _readCts = null;
        _readTask = null;
        _pipe = null;

        try { cts?.Cancel(); } catch { }
        try { pipe?.Dispose(); } catch { }
        FailPendingSends(new IOException("Flux Capturer transport session was closed."));
        if (readTask is not null)
        {
            try { await readTask.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false); }
            catch { }
        }
        cts?.Dispose();
        _pendingTxAcks.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeConnectionAsync().ConfigureAwait(false);
        _writeGate.Dispose();
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
