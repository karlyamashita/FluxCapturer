using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.IO.Ports;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FluxCapturer;

public sealed class FluxCapturerServer : IDisposable
{
    public const string SharedMemoryName = @"Local\KTYLabs_FluxCapturer_Shared_v1";

    private const uint Magic = 0x584C5546;
    private const uint Version = 1;
    private const int RxMaxPackets = 1 << 20;
    private const int RxDataBytes = 32 * 1024 * 1024;
    private const int TxMaxPackets = 65_536;
    private const int TxDataBytes = 8 * 1024 * 1024;

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    private struct PacketMeta
    {
        public ulong Sequence;
        public ulong QpcTimestamp;
        public uint DataOffset;
        public ushort PayloadLength;
        public ushort Reserved;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    private struct Header
    {
        public uint Magic;
        public uint Version;
        public ulong QpcFrequency;
        public long PacketWrite, PacketRead, PacketsUsed;
        public long DataWrite, DataRead, BytesUsed;
        public long TotalPackets, TotalBytes, Overruns;
        public long TxPacketWrite, TxPacketRead, TxPacketsUsed;
        public long TxDataWrite, TxDataRead, TxBytesUsed;
        public long TxTotalPackets, TxTotalBytes, TxOverruns;
        public uint PacketCapacity, DataCapacity, TxPacketCapacity, TxDataCapacity;
    }

    private static readonly int HeaderSize = Marshal.SizeOf<Header>();
    private static readonly int MetaSize = Marshal.SizeOf<PacketMeta>();
    private static readonly long MapSize =
        HeaderSize +
        (long)MetaSize * RxMaxPackets +
        RxDataBytes +
        (long)MetaSize * TxMaxPackets +
        TxDataBytes;

    private SerialPort? _serialPort;
    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _view;
    private unsafe byte* _base;
    private bool _pointerAcquired;
    private bool _disposed;

    public FluxCapturerServer(string portName, int baudRate)
    {
        PortName = string.IsNullOrWhiteSpace(portName)
            ? throw new ArgumentException("A COM port is required.", nameof(portName))
            : portName;
        BaudRate = baudRate > 0
            ? baudRate
            : throw new ArgumentOutOfRangeException(nameof(baudRate));

        _serialPort = CreateSerialPort(PortName, BaudRate);
    }

    public FluxCapturerServer()
    {
        PortName = string.Empty;
        BaudRate = 0;
    }

    private static SerialPort CreateSerialPort(string portName, int baudRate) =>
        new(portName, baudRate, Parity.None, 8, StopBits.One)
        {
            Handshake = Handshake.None,
            ReadTimeout = 250,
            WriteTimeout = 1_000,
            ReadBufferSize = 1 << 20,
            WriteBufferSize = 1 << 16
        };

    public string PortName { get; }
    public int BaudRate { get; }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        CreateSharedMemory();

        if (_serialPort is null)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            return;
        }

        _serialPort!.Open();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task receiveTask = ReceiveLoopAsync(linked.Token);
        Task transmitTask = TransmitLoopAsync(linked.Token);

        Task completed = await Task.WhenAny(receiveTask, transmitTask).ConfigureAwait(false);
        linked.Cancel();

        try
        {
            await Task.WhenAll(receiveTask, transmitTask).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (linked.IsCancellationRequested)
        {
            if (completed.IsFaulted)
                await completed.ConfigureAwait(false);
        }
    }

    private unsafe void CreateSharedMemory()
    {
        _mmf = MemoryMappedFile.CreateNew(
            SharedMemoryName,
            MapSize,
            MemoryMappedFileAccess.ReadWrite);
        _view = _mmf.CreateViewAccessor(0, MapSize, MemoryMappedFileAccess.ReadWrite);

        byte* pointer = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
        _pointerAcquired = true;
        _base = pointer + _view.PointerOffset;

        new Span<byte>(_base, checked((int)HeaderSize)).Clear();
        ref Header header = ref SharedHeader;
        header.Magic = Magic;
        header.Version = Version;
        header.QpcFrequency = checked((ulong)Stopwatch.Frequency);
        header.PacketCapacity = RxMaxPackets;
        header.DataCapacity = RxDataBytes;
        header.TxPacketCapacity = TxMaxPackets;
        header.TxDataCapacity = TxDataBytes;
        Thread.MemoryBarrier();
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        byte[] readBuffer = new byte[16 * 1024];
        byte[] pending = new byte[32 * 1024];
        int pendingCount = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            int read = await _serialPort!.BaseStream
                .ReadAsync(readBuffer.AsMemory(), cancellationToken)
                .ConfigureAwait(false);

            if (read <= 0)
                continue;

            if (pendingCount + read > pending.Length)
                Array.Resize(ref pending, Math.Max(pending.Length * 2, pendingCount + read));

            Buffer.BlockCopy(readBuffer, 0, pending, pendingCount, read);
            pendingCount += read;

            int consumed = 0;
            while (true)
            {
                // STM32 USB framing:
                // 55 AA | uint16 little-endian size | ID + data | checksum
                // FC only separates complete frames; CC interprets ID/data/checksum.
                while (pendingCount - consumed >= 2 &&
                       (pending[consumed] != 0x55 ||
                        pending[consumed + 1] != 0xAA))
                {
                    consumed++;
                }

                if (pendingCount - consumed < 4)
                    break;

                int payloadSize =
                    pending[consumed + 2] |
                    (pending[consumed + 3] << 8);

                if (payloadSize < 1)
                {
                    // Invalid size. Advance one byte and look for the next preamble.
                    consumed++;
                    continue;
                }

                int packetSize =
                    4 + payloadSize + 1;

                if (pendingCount - consumed < packetSize)
                    break;

                EnqueueRx(
                    pending.AsSpan(
                        consumed,
                        packetSize));

                consumed +=
                    packetSize;
            }

            if (consumed > 0)
            {
                int remaining = pendingCount - consumed;
                if (remaining > 0)
                    Buffer.BlockCopy(pending, consumed, pending, 0, remaining);
                pendingCount = remaining;
            }
        }
    }

    private async Task TransmitLoopAsync(CancellationToken cancellationToken)
    {
        System.Diagnostics.Debugger.Break();

        while (!cancellationToken.IsCancellationRequested)
        {
            bool sentAny = false;
            while (TryDequeueTx(out byte[] packet))
            {
                sentAny = true;
                await _serialPort!.BaseStream
                    .WriteAsync(packet.AsMemory(), cancellationToken)
                    .ConfigureAwait(false);
            }

            if (sentAny)
            {
                await _serialPort!.BaseStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private unsafe void EnqueueRx(ReadOnlySpan<byte> payload)
    {
        if (payload.Length == 0 || payload.Length > ushort.MaxValue)
            return;

        ref Header h = ref SharedHeader;
        long packetsUsed = Volatile.Read(ref h.PacketsUsed);
        long bytesUsed = Volatile.Read(ref h.BytesUsed);
        if ((ulong)packetsUsed >= h.PacketCapacity || bytesUsed + payload.Length > h.DataCapacity)
        {
            Interlocked.Increment(ref h.Overruns);
            return;
        }

        long packetWrite = Volatile.Read(ref h.PacketWrite);
        long dataWrite = Volatile.Read(ref h.DataWrite);
        WriteWrapped(RxData, checked((int)h.DataCapacity), checked((int)dataWrite), payload);

        ref PacketMeta meta = ref RxMeta[packetWrite];
        meta.Sequence = checked((ulong)(Volatile.Read(ref h.TotalPackets) + 1));
        meta.QpcTimestamp = checked((ulong)Stopwatch.GetTimestamp());
        meta.DataOffset = checked((uint)dataWrite);
        meta.PayloadLength = checked((ushort)payload.Length);
        meta.Reserved = 0;
        Thread.MemoryBarrier();

        Interlocked.Exchange(ref h.PacketWrite, (packetWrite + 1) % h.PacketCapacity);
        Interlocked.Exchange(ref h.DataWrite, (dataWrite + payload.Length) % h.DataCapacity);
        Interlocked.Increment(ref h.PacketsUsed);
        Interlocked.Add(ref h.BytesUsed, payload.Length);
        Interlocked.Increment(ref h.TotalPackets);
        Interlocked.Add(ref h.TotalBytes, payload.Length);
    }

    private unsafe bool TryDequeueTx(out byte[] payload)
    {
        ref Header h = ref SharedHeader;
        if (Volatile.Read(ref h.TxPacketsUsed) <= 0)
        {
            payload = Array.Empty<byte>();
            return false;
        }

        long index = Volatile.Read(ref h.TxPacketRead);
        PacketMeta meta = TxMeta[index];
        payload = new byte[meta.PayloadLength];
        ReadWrapped(TxData, checked((int)h.TxDataCapacity), checked((int)meta.DataOffset), payload);
        Thread.MemoryBarrier();

        Interlocked.Exchange(ref h.TxPacketRead, (index + 1) % h.TxPacketCapacity);
        Interlocked.Exchange(ref h.TxDataRead, ((long)meta.DataOffset + meta.PayloadLength) % h.TxDataCapacity);
        Interlocked.Decrement(ref h.TxPacketsUsed);
        Interlocked.Add(ref h.TxBytesUsed, -meta.PayloadLength);
        return true;
    }

    private unsafe ref Header SharedHeader => ref Unsafe.AsRef<Header>(_base);
    private unsafe PacketMeta* RxMeta => (PacketMeta*)(_base + HeaderSize);
    private unsafe byte* RxData => _base + HeaderSize + MetaSize * RxMaxPackets;
    private unsafe PacketMeta* TxMeta => (PacketMeta*)(RxData + RxDataBytes);
    private unsafe byte* TxData => (byte*)(TxMeta + TxMaxPackets);

    private static unsafe void ReadWrapped(byte* source, int capacity, int offset, Span<byte> destination)
    {
        if (destination.Length == 0)
            return;

        int first = Math.Min(destination.Length, capacity - offset);
        fixed (byte* destinationPointer = destination)
        {
            Buffer.MemoryCopy(source + offset, destinationPointer, destination.Length, first);
            if (destination.Length > first)
            {
                Buffer.MemoryCopy(
                    source,
                    destinationPointer + first,
                    destination.Length - first,
                    destination.Length - first);
            }
        }
    }

    private static unsafe void WriteWrapped(byte* destination, int capacity, int offset, ReadOnlySpan<byte> source)
    {
        int first = Math.Min(source.Length, capacity - offset);
        fixed (byte* sourcePointer = source)
        {
            Buffer.MemoryCopy(sourcePointer, destination + offset, first, first);
            if (source.Length > first)
            {
                Buffer.MemoryCopy(
                    sourcePointer + first,
                    destination,
                    source.Length - first,
                    source.Length - first);
            }
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    public unsafe void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        if (_serialPort?.IsOpen == true)
            _serialPort!.Close();
        _serialPort?.Dispose();

        if (_view is not null && _pointerAcquired)
        {
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _pointerAcquired = false;
        }

        _base = null;
        _view?.Dispose();
        _view = null;
        _mmf?.Dispose();
        _mmf = null;
    }
}
