namespace FluxCapturer.Client;

public sealed class FluxCapturedPacket
{
    public ulong Sequence { get; init; }
    public ulong QpcTimestamp { get; init; }
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}
