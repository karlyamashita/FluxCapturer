namespace FluxCapturer.Client;

public sealed class FluxCapturerProcessLease : IDisposable
{
    private readonly int _ownedProcessId;
    private int _disposed;

    internal FluxCapturerProcessLease(int ownedProcessId)
    {
        _ownedProcessId = ownedProcessId;
    }

    public bool OwnsProcess => _ownedProcessId != 0;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        if (_ownedProcessId != 0)
            FluxCapturerProcessManager.Release(_ownedProcessId);
    }
}
