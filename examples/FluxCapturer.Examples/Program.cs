using FluxCapturer.Client;
using System.IO.Ports;

string portName = args.Length >= 1 ? args[0] : "COM5";
int baudRate = args.Length >= 2 && int.TryParse(args[1], out int baud)
    ? baud
    : 115200;

Console.WriteLine("Flux Capturer serial example");
Console.WriteLine($"Port: {portName}");
Console.WriteLine($"Baud: {baudRate}");
Console.WriteLine();

await FluxCapturerProcessManager.EnsureHostRunningAsync();

await using var session = new FluxTransportSession();

session.DataReceived += (_, data) =>
{
    Console.WriteLine(
        $"{DateTime.Now:HH:mm:ss.fff} RX ({data.Length}) " +
        BitConverter.ToString(data).Replace("-", " "));
};

session.Faulted += (_, message) =>
{
    Console.Error.WriteLine($"FC ERROR: {message}");
};

await session.OpenSerialAsync(
    portName: portName,
    baudRate: baudRate,
    parity: (int)Parity.None,
    dataBits: 8,
    stopBits: (int)StopBits.One,
    handshake: (int)Handshake.None,
    dtrEnable: false,
    rtsEnable: false,
    rs485Rts: false);

Console.WriteLine($"Connected: {session.IsConnected}");
Console.WriteLine();
Console.WriteLine("Enter hexadecimal bytes to transmit.");
Console.WriteLine("Example: 55 AA 01 00 90 90");
Console.WriteLine("Press Enter on an empty line to quit.");
Console.WriteLine();

while (true)
{
    Console.Write("TX> ");
    string? line = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(line))
        break;

    try
    {
        byte[] bytes = ParseHex(line);
        await session.SendAsync(bytes);

        Console.WriteLine(
            $"TX ACK ({bytes.Length}) " +
            BitConverter.ToString(bytes).Replace("-", " "));
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);
    }
}

static byte[] ParseHex(string text)
{
    string[] tokens = text.Split(
        new[] { ' ', '\t', ',', '-', ':' },
        StringSplitOptions.RemoveEmptyEntries);

    if (tokens.Length == 0)
        throw new FormatException("No hexadecimal bytes were entered.");

    byte[] bytes = new byte[tokens.Length];

    for (int i = 0; i < tokens.Length; i++)
    {
        string token = tokens[i];

        if (token.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
            token = token[2..];

        if (!byte.TryParse(
            token,
            System.Globalization.NumberStyles.HexNumber,
            System.Globalization.CultureInfo.InvariantCulture,
            out bytes[i]))
        {
            throw new FormatException(
                $"'{tokens[i]}' is not a valid hexadecimal byte.");
        }
    }

    return bytes;
}
