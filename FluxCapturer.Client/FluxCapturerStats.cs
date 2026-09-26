namespace FluxCapturer.Client;

public sealed class FluxCapturerStats
{
    public long RxQueuePackets { get; init; }
    public long RxBytesUsed { get; init; }
    public long RxTotalPackets { get; init; }
    public long RxTotalBytes { get; init; }
    public long RxOverruns { get; init; }
    public long TxQueuePackets { get; init; }
    public long TxBytesUsed { get; init; }
    public long TxTotalPackets { get; init; }
    public long TxTotalBytes { get; init; }
    public long TxOverruns { get; init; }
    public ulong QpcFrequency { get; init; }
}
