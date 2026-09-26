# Flux Capturer .NET API

Flux Capturer separates transport I/O from the consuming application. The current
public .NET client library is `FluxCapturer.Client`.

This document describes the API that exists in the current source. Flux Capturer
does not currently decode CAN messages or STM32 command IDs for the application.
It transports complete byte frames; application-level decoding remains in the
consumer.

## 1. Add the client project

During development, add a project reference to:

```text
FluxCapturer.Client/FluxCapturer.Client.csproj
```

Example:

```xml
<ItemGroup>
  <ProjectReference Include="..\..\FluxCapturer.Client\FluxCapturer.Client.csproj" />
</ItemGroup>
```

The client targets `net8.0-windows` and is `AnyCPU`.

## 2. Which API should I use?

There are three public entry points.

| API | Use it for |
|---|---|
| `FluxTransportSession` | Open serial/TCP through the FC host, send bytes, receive bytes |
| `FluxCapturerProcessManager` | Start/find the FC host and manage legacy direct-serial leases |
| `FluxCapturerClient` | Read/write the high-throughput shared-memory queues directly |

For a new application that wants FC to own a COM port or TCP connection,
`FluxTransportSession` is the normal starting point.

---

# 5-minute serial example

This opens `COM5` at 115200, prints received bytes, sends one byte sequence, and
closes cleanly.

```csharp
using FluxCapturer.Client;
using System.IO.Ports;

await FluxCapturerProcessManager.EnsureHostRunningAsync();

await using var session = new FluxTransportSession();

session.DataReceived += (_, data) =>
{
    Console.WriteLine($"RX ({data.Length}): {Convert.ToHexString(data)}");
};

session.Faulted += (_, error) =>
{
    Console.Error.WriteLine($"FC ERROR: {error}");
};

await session.OpenSerialAsync(
    portName: "COM5",
    baudRate: 115200,
    parity: (int)Parity.None,
    dataBits: 8,
    stopBits: (int)StopBits.One,
    handshake: (int)Handshake.None,
    dtrEnable: false,
    rtsEnable: false,
    rs485Rts: false);

Console.WriteLine($"Connected: {session.IsConnected}");

await session.SendAsync(new byte[] { 0x55, 0xAA, 0x01, 0x00, 0x90, 0x90 });

Console.WriteLine("Press Enter to quit.");
Console.ReadLine();
```

`OpenSerialAsync()` currently accepts the serial enum values as integers. Using
the `System.IO.Ports` enums as shown above makes the meaning explicit.

---

# Starting the host

## Application-managed host

```csharp
await FluxCapturerProcessManager.EnsureHostRunningAsync();
```

This returns immediately when the FC shared-memory host is already available.
Otherwise it locates and starts `FluxCapturer.exe`, then waits up to five seconds
for the host to publish its shared memory.

The process manager currently looks for:

```text
<application directory>\FluxCapturer\FluxCapturer.exe
<application directory>\FluxCapturer.exe
```

A typical application flow is:

```csharp
await FluxCapturerProcessManager.EnsureHostRunningAsync();

await using var session = new FluxTransportSession();
// Open serial or TCP here.
```

## Standalone host

For testing FC without Serial Flux or CAN Coyote X7:

```text
FluxCapturer.exe --standalone
```

Your application can then create a `FluxTransportSession` and open the desired
transport.

---

# Serial transport

## Open a normal 8-N-1 COM port

```csharp
using FluxCapturer.Client;
using System.IO.Ports;

await using var session = new FluxTransportSession();

await session.OpenSerialAsync(
    "COM7",
    115200,
    (int)Parity.None,
    8,
    (int)StopBits.One,
    (int)Handshake.None,
    false,
    false,
    false);
```

## Open with DTR and RTS enabled

```csharp
await session.OpenSerialAsync(
    portName: "COM7",
    baudRate: 921600,
    parity: (int)Parity.None,
    dataBits: 8,
    stopBits: (int)StopBits.One,
    handshake: (int)Handshake.None,
    dtrEnable: true,
    rtsEnable: true,
    rs485Rts: false);
```

## RS-485 RTS option

The current API exposes `rs485Rts`:

```csharp
await session.OpenSerialAsync(
    "COM9",
    115200,
    (int)Parity.None,
    8,
    (int)StopBits.One,
    (int)Handshake.None,
    false,
    false,
    true);
```

The exact electrical behavior still depends on the serial adapter/driver.

---

# Receiving data

`FluxTransportSession.DataReceived` delivers the byte payload received from the
transport.

```csharp
session.DataReceived += (_, data) =>
{
    Console.WriteLine(
        $"{DateTime.Now:HH:mm:ss.fff} RX {data.Length} bytes: " +
        Convert.ToHexString(data));
};
```

If you need to retain the bytes, copy them or place them in your own queue from
the event handler.

```csharp
var rxQueue = new System.Collections.Concurrent.ConcurrentQueue<byte[]>();

session.DataReceived += (_, data) =>
{
    rxQueue.Enqueue(data.ToArray());
};
```

Keep the event handler short if the application receives data continuously.

---

# Sending data

```csharp
byte[] tx = { 0x01, 0x02, 0x03, 0x04 };

await session.SendAsync(tx);
```

`SendAsync()` waits for the FC host's transmit acknowledgement before it
completes. It throws if the transport is not connected.

With cancellation:

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

await session.SendAsync(
    new byte[] { 0x55, 0xAA },
    cts.Token);
```

---

# TCP transport

```csharp
await FluxCapturerProcessManager.EnsureHostRunningAsync();

await using var session = new FluxTransportSession();

session.DataReceived += (_, data) =>
{
    Console.WriteLine($"TCP RX: {Convert.ToHexString(data)}");
};

await session.OpenTcpAsync("192.168.1.50", 5000);

await session.SendAsync(
    System.Text.Encoding.ASCII.GetBytes("hello\r\n"));
```

---

# Connection and fault handling

Check the current session state with:

```csharp
if (session.IsConnected)
{
    await session.SendAsync(data);
}
```

Subscribe to `Faulted` before opening the connection:

```csharp
session.Faulted += (_, message) =>
{
    Console.Error.WriteLine($"Flux Capturer fault: {message}");
};
```

Opening a new serial or TCP transport on the same `FluxTransportSession`
disposes its previous connection first.

---

# Clean shutdown

`FluxTransportSession` implements `IAsyncDisposable`, so prefer:

```csharp
await using var session = new FluxTransportSession();
```

or:

```csharp
var session = new FluxTransportSession();

try
{
    // Use session.
}
finally
{
    await session.DisposeAsync();
}
```

---

# Shared-memory capture API

`FluxCapturerClient` provides direct access to FC's shared-memory RX/TX queues.
This is useful for applications using the existing high-throughput capture path.

## Check whether FC is available

```csharp
if (FluxCapturerClient.IsAvailable())
{
    Console.WriteLine("Flux Capturer shared memory is available.");
}
```

## Connect

```csharp
using var client = new FluxCapturerClient();

client.Connect();

Console.WriteLine(client.IsConnected);
```

`Connect()` verifies the shared-memory magic and version. A protocol mismatch
throws `InvalidOperationException`.

## Read one captured packet

```csharp
if (client.TryReadPacket(out FluxCapturedPacket packet))
{
    Console.WriteLine($"Sequence: {packet.Sequence}");
    Console.WriteLine($"QPC:      {packet.QpcTimestamp}");
    Console.WriteLine($"Payload:  {Convert.ToHexString(packet.Payload)}");
}
```

`TryReadPacket()` removes the packet from the shared RX queue.

## Read a batch

```csharp
var packets = new List<FluxCapturedPacket>();

int count = client.ReadPackets(
    maxPackets: 100,
    destination: packets);

foreach (FluxCapturedPacket packet in packets)
{
    Console.WriteLine(
        $"{packet.Sequence}: {Convert.ToHexString(packet.Payload)}");
}
```

## Convert QPC timestamp to seconds

```csharp
double seconds = client.QpcToSeconds(packet.QpcTimestamp);
```

This converts the packet's QPC ticks using the frequency published in the
shared-memory header.

## Get queue statistics

```csharp
FluxCapturerStats stats = client.GetStats();

Console.WriteLine($"RX queued packets : {stats.RxQueuePackets}");
Console.WriteLine($"RX queued bytes   : {stats.RxBytesUsed}");
Console.WriteLine($"RX total packets  : {stats.RxTotalPackets}");
Console.WriteLine($"RX total bytes    : {stats.RxTotalBytes}");
Console.WriteLine($"RX overruns       : {stats.RxOverruns}");

Console.WriteLine($"TX queued packets : {stats.TxQueuePackets}");
Console.WriteLine($"TX queued bytes   : {stats.TxBytesUsed}");
Console.WriteLine($"TX total packets  : {stats.TxTotalPackets}");
Console.WriteLine($"TX total bytes    : {stats.TxTotalBytes}");
Console.WriteLine($"TX overruns       : {stats.TxOverruns}");

Console.WriteLine($"QPC frequency     : {stats.QpcFrequency}");
```

## Queue bytes for transmission

```csharp
bool queued = client.TrySend(
    new byte[] { 0x01, 0x02, 0x03 });

if (!queued)
{
    Console.WriteLine("TX queue is full or the payload was invalid.");
}
```

`TrySend()` returns `false` when the payload is empty, exceeds 65535 bytes, or
there is not enough room in the shared TX queue.

---

# Legacy direct-serial process lease

The current source also contains `FluxCapturerProcessManager.AcquireAsync()`.
This starts the legacy FC direct-serial host form:

```text
FluxCapturer.exe COM5 115200
```

Example:

```csharp
using FluxCapturer.Client;

using FluxCapturerProcessLease lease =
    await FluxCapturerProcessManager.AcquireAsync(
        "COM5",
        115200);

Console.WriteLine($"This caller owns FC process: {lease.OwnsProcess}");

using var client = new FluxCapturerClient();
client.Connect();

// Read shared-memory packets here.
```

Dispose the lease when finished. If the lease owns the FC process and it is the
last lease, the process manager stops that owned process.

For new transport-oriented code, prefer `EnsureHostRunningAsync()` plus
`FluxTransportSession`.

---

# STM32 frame example

The current FC server recognizes this STM32 envelope:

```text
55 AA <payload-size-LE16> <payload> <checksum>
```

For example, this C# helper constructs the envelope. The checksum algorithm is
application/protocol specific and is intentionally supplied by the caller:

```csharp
static byte[] BuildStm32Frame(
    ReadOnlySpan<byte> payload,
    byte checksum)
{
    if (payload.Length > ushort.MaxValue)
        throw new ArgumentOutOfRangeException(nameof(payload));

    byte[] frame = new byte[2 + 2 + payload.Length + 1];

    frame[0] = 0x55;
    frame[1] = 0xAA;

    ushort length = checked((ushort)payload.Length);
    frame[2] = (byte)(length & 0xFF);
    frame[3] = (byte)(length >> 8);

    payload.CopyTo(frame.AsSpan(4));
    frame[^1] = checksum;

    return frame;
}
```

FC currently separates complete frames but does not interpret the command ID,
CAN ID, CAN data, or checksum semantics.

---

# Complete console example

A complete compilable example is included in:

```text
examples/FluxCapturer.Examples/
```

Run it with:

```text
dotnet run --project examples/FluxCapturer.Examples -- COM5 115200
```

It:

1. Ensures the FC host is running.
2. Opens the requested COM port.
3. Displays received data as hex.
4. Lets you type hexadecimal bytes to transmit.
5. Reports transport faults.
6. Shuts the session down cleanly.

---

# Current API limitations

The current repository does **not** yet expose high-level methods such as:

```text
OpenCan()
SetCanBaud()
SendCanFrame()
ReceiveCanFrame()
GetDeviceUid()
GetFirmwareVersion()
```

Those operations require the consuming application to understand the STM32
payload protocol. They should not be documented as existing FC APIs until the
corresponding public wrappers are implemented.

Likewise, Python and `python-can` bindings are planned layers, not part of the
current public source.

---

# API stability

Version 1.0 preserves the API currently used by KTY applications. Higher-level
CAN-specific wrappers, Python bindings, and `python-can` integration should be
layered on top rather than changing the v1 transport contract.

Breaking changes to the shared-memory layout, named-pipe framing, or IPC names
described in `PROTOCOL.md` should be treated as protocol compatibility changes.
