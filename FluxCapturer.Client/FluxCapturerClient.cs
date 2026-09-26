using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FluxCapturer.Client;

public sealed unsafe class FluxCapturerClient : IDisposable
{
    public const string SharedMemoryName = @"Local\KTYLabs_FluxCapturer_Shared_v1";
    private const uint Magic = 0x584C5546;
    private const uint Version = 1;
    private const int RxMaxPackets = 1 << 20;
    private const int RxDataBytes = 32 * 1024 * 1024;
    private const int TxMaxPackets = 65536;

    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _view;
    private byte* _base;
    private bool _pointerAcquired;

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

    private static readonly int HeaderSize = sizeof(Header);
    private static readonly int MetaSize = sizeof(PacketMeta);

    public bool IsConnected => _base != null;

    public static bool IsAvailable()
    {
        try
        {
            using var map = MemoryMappedFile.OpenExisting(SharedMemoryName, MemoryMappedFileRights.Read);
            return true;
        }
        catch { return false; }
    }

    public void Connect()
    {
        Dispose();
        _mmf = MemoryMappedFile.OpenExisting(SharedMemoryName, MemoryMappedFileRights.ReadWrite);
        _view = _mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);
        byte* pointer = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
        _pointerAcquired = true;
        _base = pointer + _view.PointerOffset;

        if (SharedHeader.Magic != Magic || SharedHeader.Version != Version)
        {
            Dispose();
            throw new InvalidOperationException("Flux Capturer shared-memory protocol mismatch.");
        }
    }

    public FluxCapturerStats GetStats()
    {
        EnsureConnected();
        ref Header h = ref SharedHeader;
        return new FluxCapturerStats
        {
            RxQueuePackets = Volatile.Read(ref h.PacketsUsed),
            RxBytesUsed = Volatile.Read(ref h.BytesUsed),
            RxTotalPackets = Volatile.Read(ref h.TotalPackets),
            RxTotalBytes = Volatile.Read(ref h.TotalBytes),
            RxOverruns = Volatile.Read(ref h.Overruns),
            TxQueuePackets = Volatile.Read(ref h.TxPacketsUsed),
            TxBytesUsed = Volatile.Read(ref h.TxBytesUsed),
            TxTotalPackets = Volatile.Read(ref h.TxTotalPackets),
            TxTotalBytes = Volatile.Read(ref h.TxTotalBytes),
            TxOverruns = Volatile.Read(ref h.TxOverruns),
            QpcFrequency = h.QpcFrequency
        };
    }

    public int ReadPackets(int maxPackets, List<FluxCapturedPacket> destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        int count = 0;
        while (count < maxPackets && TryReadPacket(out FluxCapturedPacket? packet))
        {
            destination.Add(packet);
            count++;
        }
        return count;
    }

    public bool TryReadPacket(out FluxCapturedPacket packet)
    {
        EnsureConnected();
        ref Header h = ref SharedHeader;
        if (Volatile.Read(ref h.PacketsUsed) <= 0)
        {
            packet = null!;
            return false;
        }

        long index = Volatile.Read(ref h.PacketRead);
        PacketMeta meta = RxMeta[index];
        byte[] payload = new byte[meta.PayloadLength];
        ReadWrapped(RxData, checked((int)h.DataCapacity), checked((int)meta.DataOffset), payload);
        Thread.MemoryBarrier();

        Interlocked.Exchange(ref h.PacketRead, (index + 1) % h.PacketCapacity);
        Interlocked.Exchange(ref h.DataRead, ((long)meta.DataOffset + meta.PayloadLength) % h.DataCapacity);
        Interlocked.Decrement(ref h.PacketsUsed);
        Interlocked.Add(ref h.BytesUsed, -meta.PayloadLength);

        packet = new FluxCapturedPacket
        {
            Sequence = meta.Sequence,
            QpcTimestamp = meta.QpcTimestamp,
            Payload = payload
        };
        return true;
    }

    public bool TrySend(ReadOnlySpan<byte> bytes)
    {
        EnsureConnected();
        if (bytes.Length == 0 || bytes.Length > ushort.MaxValue) return false;

        ref Header h = ref SharedHeader;
        long packetsUsed = Volatile.Read(ref h.TxPacketsUsed);
        long bytesUsed = Volatile.Read(ref h.TxBytesUsed);
        if ((ulong)packetsUsed >= h.TxPacketCapacity || bytesUsed + bytes.Length > h.TxDataCapacity)
        {
            Interlocked.Increment(ref h.TxOverruns);
            return false;
        }

        long packetWrite = Volatile.Read(ref h.TxPacketWrite);
        long dataWrite = Volatile.Read(ref h.TxDataWrite);
        WriteWrapped(TxData, checked((int)h.TxDataCapacity), checked((int)dataWrite), bytes);

        ref PacketMeta meta = ref TxMeta[packetWrite];
        meta.Sequence = (ulong)(Volatile.Read(ref h.TxTotalPackets) + 1);
        meta.QpcTimestamp = 0;
        meta.DataOffset = checked((uint)dataWrite);
        meta.PayloadLength = checked((ushort)bytes.Length);
        meta.Reserved = 0;
        Thread.MemoryBarrier();

        Interlocked.Exchange(ref h.TxPacketWrite, (packetWrite + 1) % h.TxPacketCapacity);
        Interlocked.Exchange(ref h.TxDataWrite, (dataWrite + bytes.Length) % h.TxDataCapacity);
        Interlocked.Increment(ref h.TxPacketsUsed);
        Interlocked.Add(ref h.TxBytesUsed, bytes.Length);
        Interlocked.Increment(ref h.TxTotalPackets);
        Interlocked.Add(ref h.TxTotalBytes, bytes.Length);
        return true;
    }

    public double QpcToSeconds(ulong ticks)
    {
        EnsureConnected();
        ulong frequency = SharedHeader.QpcFrequency;
        return frequency == 0 ? 0 : ticks / (double)frequency;
    }

    private ref Header SharedHeader => ref Unsafe.AsRef<Header>(_base);
    private PacketMeta* RxMeta => (PacketMeta*)(_base + HeaderSize);
    private byte* RxData => _base + HeaderSize + MetaSize * RxMaxPackets;
    private PacketMeta* TxMeta => (PacketMeta*)(RxData + RxDataBytes);
    private byte* TxData => (byte*)(TxMeta + TxMaxPackets);

    private static void ReadWrapped(byte* source, int capacity, int offset, Span<byte> destination)
    {
        if (destination.Length == 0) return;
        int first = Math.Min(destination.Length, capacity - offset);
        fixed (byte* dst = destination)
        {
            Buffer.MemoryCopy(source + offset, dst, destination.Length, first);
            if (destination.Length > first)
                Buffer.MemoryCopy(source, dst + first, destination.Length - first, destination.Length - first);
        }
    }

    private static void WriteWrapped(byte* destination, int capacity, int offset, ReadOnlySpan<byte> source)
    {
        int first = Math.Min(source.Length, capacity - offset);
        fixed (byte* src = source)
        {
            Buffer.MemoryCopy(src, destination + offset, first, first);
            if (source.Length > first)
                Buffer.MemoryCopy(src + first, destination, source.Length - first, source.Length - first);
        }
    }

    private void EnsureConnected()
    {
        if (_base == null) throw new InvalidOperationException("Flux Capturer is not connected.");
    }

    public void Dispose()
    {
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
