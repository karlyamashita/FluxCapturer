using System.Globalization;
using System.Text;

namespace FluxCapturer;

/// <summary>Passive observer of Pico LOGIC_* serial replies. Never reads or writes the COM port.</summary>
internal static class PicoIlaCapture
{
    private sealed class PortState
    {
        public readonly StringBuilder Line = new();
        public bool InCapture;
        public int Id, Hz, Expected;
        public bool EdgeTicks;
        public int Packet;
        public bool Stream;
        public readonly List<string> Edges = new();
        public long LastTick = -1;
        public readonly StringBuilder Hex = new();
    }

    private static readonly object Gate = new();
    private static Task forwardTail = Task.CompletedTask;
    private static readonly Dictionary<string, PortState> Ports = new(StringComparer.OrdinalIgnoreCase);
    private static string[]? lastTransitions;
    private static string lastPort = "";
    private static int lastSamples, lastHz;
    private static string lastError = "NO_CAPTURE";
    private const int MaxSamples = 100_000;


    internal static string[] KnownPorts()
    {
        lock (Gate) return Ports.Where(p => p.Value.Hz > 0).Select(p => p.Key).ToArray();
    }

    internal static bool IsKnownPicoPort(string port)
    {
        lock (Gate) return Ports.TryGetValue(port, out var state) && state.Id >= 0 && state.Hz > 0;
    }

    public static void OnSerialBytes(string port, ReadOnlySpan<byte> bytes)
    {
        var readyPackets = new List<(List<string> Data, int Session, int Number)>();
        lock (Gate)
        {
            if (!Ports.TryGetValue(port, out var state)) Ports[port] = state = new PortState();
            foreach (byte b in bytes)
            {
                if (b == (byte)'\n')
                {
                    string line = state.Line.ToString().Trim();
                    state.Line.Clear();
                    if (line.Length != 0) {
                        var completed = ProcessLine(port, state, line);
                        if (completed is not null) {
                            readyPackets.Add((completed, state.Id, state.Stream ? state.Packet : -1));
                        }
                    }
                }
                else if (b != (byte)'\r')
                {
                    if (b is >= 32 and <= 126 && state.Line.Length < 8192)
                        state.Line.Append((char)b);
                    else if (state.Line.Length >= 8192)
                    {
                        state.Line.Clear();
                        state.InCapture = false;
                        lastError = "LINE_TOO_LONG";
                    }
                }
            }
        }
        if (readyPackets.Count != 0)
        {
            lock (Gate) {
                foreach (var ready in readyPackets) {
                    var packet = ready.Data;
                    int session = ready.Session, number = ready.Number;
                    forwardTail = forwardTail.ContinueWith(async _ => {
                        try {
                            if (number > 0)
                                await IlaBridgeHost.SendPacketAsync(session, number, packet, CancellationToken.None).ConfigureAwait(false);
                            else
                                await IlaBridgeHost.SendCaptureAsync(packet, CancellationToken.None).ConfigureAwait(false);
                        } catch (Exception ex) { Console.Error.WriteLine("Pico ILA forward: " + ex.Message); }
                    }, TaskScheduler.Default).Unwrap();
                }
            }
        }
    }

    private static List<string>? ProcessLine(string port, PortState s, string line)
    {
        if (line.StartsWith("LOGIC_BEGIN I2C ", StringComparison.Ordinal))
        {
            s.InCapture = false;
            s.Hex.Clear();
            s.Edges.Clear();
            s.LastTick = -1;
            int id = Field(line, "ID"), hz = Field(line, "SAMPLE_HZ"), count = Field(line, "COUNT");
            if (id < 0 || hz <= 0 || count is < 2 or > MaxSamples ||
                !(line.Contains("FORMAT=2BIT_HEX", StringComparison.Ordinal) ||
                  line.Contains("FORMAT=EDGE_TICKS", StringComparison.Ordinal)))
            { lastError = "INVALID_BEGIN"; return null; }
            s.Id = id; s.Hz = hz; s.Expected = count; s.InCapture = true;
            s.Packet = Field(line, "PACKET");
            s.Stream = s.Packet > 0;
            s.EdgeTicks = line.Contains("FORMAT=EDGE_TICKS", StringComparison.Ordinal);
            return null;
        }
        if (!s.InCapture) return null;
        if (s.EdgeTicks && line.StartsWith("LOGIC_EDGE ", StringComparison.Ordinal))
        {
            string[] fields = line[11..].Split(',');
            if (fields.Length != 2 ||
                !long.TryParse(fields[0], NumberStyles.None, CultureInfo.InvariantCulture, out long tick) ||
                !int.TryParse(fields[1], NumberStyles.None, CultureInfo.InvariantCulture, out int pins) ||
                tick < 0 || tick <= s.LastTick || pins is < 0 or > 3 || s.Edges.Count >= s.Expected)
            { s.InCapture = false; lastError = "INVALID_EDGE"; return null; }
            s.LastTick = tick;
            s.Edges.Add((tick * 1_000_000.0 / s.Hz).ToString("R", CultureInfo.InvariantCulture) +
                        "," + (pins & 1).ToString(CultureInfo.InvariantCulture) +
                        "," + ((pins >> 1) & 1).ToString(CultureInfo.InvariantCulture));
            return null;
        }
        if (line.StartsWith("LOGIC_DATA ", StringComparison.Ordinal))
        {
            string hex = line[11..].Trim();
            if (hex.Length == 0 || s.Hex.Length + hex.Length > s.Expected ||
                hex.Any(c => c is < '0' or > '3'))
            { s.InCapture = false; lastError = "INVALID_DATA"; return null; }
            s.Hex.Append(hex);
            return null;
        }
        if (line.StartsWith("LOGIC_END ", StringComparison.Ordinal))
        {
            s.InCapture = false;
            if (s.EdgeTicks)
            {
                if (Field(line, "ID") != s.Id || Field(line, "COUNT") != s.Expected ||
                    s.Edges.Count != s.Expected ||
                    (s.Stream && Field(line, "PACKET") != s.Packet) ||
                    line.Contains("OVERFLOW=1", StringComparison.Ordinal))
                { lastError = "INCOMPLETE_EDGE_CAPTURE"; return null; }
                var edges = new List<string>(s.Edges);
                lastTransitions = edges.ToArray();
                lastPort = port; lastSamples = s.Expected; lastHz = s.Hz; lastError = "NONE";
                return edges;
            }
            if (Field(line, "ID") != s.Id || Field(line, "COUNT") != s.Expected ||
                s.Hex.Length != s.Expected)
            { lastError = "INCOMPLETE_CAPTURE"; return null; }
            var result = new List<string>();
            int prev = -1;
            for (int i = 0; i < s.Hex.Length; i++)
            {
                int v = s.Hex[i] - '0';
                if (v == prev) continue;
                // 2BIT_HEX: bit0=SDA, bit1=SCL; sample interval = 1e6 / Hz microseconds.
                result.Add((i * 1_000_000.0 / s.Hz).ToString("R", CultureInfo.InvariantCulture) +
                    "," + (v & 1).ToString(CultureInfo.InvariantCulture) +
                    "," + ((v >> 1) & 1).ToString(CultureInfo.InvariantCulture));
                prev = v;
            }
            if (result.Count < 2) { lastError = "NO_EDGES"; return null; }
            // Include the final timestamp to preserve the visible idle duration.
            int final = s.Hex[^1] - '0';
            if (s.Expected > 1 && s.Hex[^1] == s.Hex[^2])
                result.Add(((s.Expected - 1) * 1_000_000.0 / s.Hz).ToString("R", CultureInfo.InvariantCulture) +
                    "," + (final & 1).ToString(CultureInfo.InvariantCulture) +
                    "," + ((final >> 1) & 1).ToString(CultureInfo.InvariantCulture));
            lastTransitions = result.ToArray();
            lastPort = port; lastSamples = s.Expected; lastHz = s.Hz; lastError = "NONE";
            return result;
        }
        return null;
    }

    private static int Field(string line, string key)
    {
        foreach (string field in line.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            if (field.StartsWith(key + "=", StringComparison.Ordinal) &&
                int.TryParse(field.AsSpan(key.Length + 1), NumberStyles.None,
                    CultureInfo.InvariantCulture, out int value)) return value;
        return -1;
    }

    public static (string[]? Samples, string Status) Snapshot()
    {
        lock (Gate)
        {
            string status = lastTransitions is null ? "PICO=NO_CAPTURE ERROR=" + lastError :
                $"PICO=CAPTURED PORT={lastPort} SAMPLES={lastSamples} SAMPLE_HZ={lastHz} TRANSITIONS={lastTransitions.Length}";
            return (lastTransitions, status);
        }
    }
}
